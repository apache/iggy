/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iggy.client.blocking.http;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.DefaultHttpRequestRetryStrategy;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpException;
import org.apache.hc.core5.http.HttpRequest;
import org.apache.hc.core5.http.HttpResponse;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.Method;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.impl.io.HttpRequestExecutor;
import org.apache.hc.core5.http.io.HttpClientConnection;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.HttpResponseInformationCallback;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.http.protocol.HttpContext;
import org.apache.hc.core5.io.CloseMode;
import org.apache.hc.core5.io.ModalCloseable;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyErrorCode;
import org.apache.iggy.exception.IggyException;
import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.exception.IggyTlsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InterruptedIOException;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

final class InternalHttpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(InternalHttpClient.class);

    private static final String AUTHORIZATION = "Authorization";
    private static final String UNCERTAIN_ATTEMPT = "iggy.uncertain-attempt";
    private static final String DEFERRED_ATTEMPTED = "iggy.deferred-attempted";
    private static final Set<IggyErrorCode> SETTLED_REFUSALS = EnumSet.of(
            IggyErrorCode.HISTORY_UNAVAILABLE,
            IggyErrorCode.LIFECYCLE_BUSY,
            IggyErrorCode.UNAUTHORIZED,
            IggyErrorCode.UNAUTHENTICATED);

    /**
     * Deferred requests occupy their connection for the whole readiness wait, so
     * they get their own pool and can never starve control traffic. The bound
     * matches the binary client's deferred lease count.
     */
    private static final int MAX_DEFERRED_CONNECTIONS = 64;

    /** Mirrors the binary client's pending-poll admission cap. */
    private static final int MAX_PENDING_DEFERRED_REQUESTS = 4096;

    private static final int BODY_CHUNK_BYTES = 8 * 1024;

    private final String url;
    private final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
    private final CloseableHttpClient httpClient;
    private final CloseableHttpClient deferredHttpClient;
    private final ScheduledThreadPoolExecutor deadlines = createDeadlineTimer();
    private final AtomicInteger pendingDeferred = new AtomicInteger();
    private Optional<String> token = Optional.empty();

    InternalHttpClient(
            String url,
            Optional<Duration> connectionTimeout,
            Optional<Duration> requestTimeout,
            Optional<File> tlsCertificate) {
        UrlValidator.validateHttpUrl(url);
        this.url = url;
        this.httpClient = createHttpClient(connectionTimeout, requestTimeout, tlsCertificate);
        this.deferredHttpClient = createDeferredHttpClient(connectionTimeout, tlsCertificate);
    }

    private static CloseableHttpClient createHttpClient(
            Optional<Duration> connectionTimeout, Optional<Duration> requestTimeout, Optional<File> tlsCertificate) {
        var connectionConfigBuilder = ConnectionConfig.custom();
        connectionTimeout.ifPresent(timeout -> connectionConfigBuilder.setConnectTimeout(Timeout.of(timeout)));

        var connectionManagerBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(connectionConfigBuilder.build());

        applyTlsCertificate(connectionManagerBuilder, tlsCertificate);

        var requestConfigBuilder = RequestConfig.custom();
        requestTimeout.ifPresent(timeout -> requestConfigBuilder.setResponseTimeout(Timeout.of(timeout)));

        return HttpClients.custom()
                .setConnectionManager(connectionManagerBuilder.build())
                .setDefaultRequestConfig(requestConfigBuilder.build())
                .setRetryStrategy(new UncertainAttempts())
                .build();
    }

    /**
     * A deferred poll declares its own total budget. The library's timeouts
     * restart for every wait, so a read that starts near the deadline would
     * outlive it. A timer cancels the exchange at the deadline instead, which
     * ends the connection lease, the connect, the wait for headers and any body
     * read. Automatic retries are off: the server answers both
     * {@code TRANSIENT_NOT_ACCEPTED} and the ambiguous
     * {@code TRANSIENT_NOT_COMMITTED} with HTTP 503, and a proxy can answer it
     * too, so only a parsed server code may authorize a replay.
     *
     * @param request       the prepared deferred request
     * @param clazz         the success body type
     * @param deadlineNanos absolute {@link System#nanoTime()} deadline for the whole exchange
     * @param maxBodyBytes  the largest encoded body this client will accumulate
     * @param <T>           the success body type
     * @return the decoded body
     */
    <T> T executeDeferred(ClassicHttpRequest request, Class<T> clazz, long deadlineNanos, long maxBodyBytes) {
        if (pendingDeferred.incrementAndGet() > MAX_PENDING_DEFERRED_REQUESTS) {
            pendingDeferred.decrementAndGet();
            throw deferredFailure(false, new IggyClientException("Too many pending deferred HTTP polls"));
        }
        ScheduledFuture<?> abort = null;
        var context = HttpClientContext.create();
        try {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new IggyTimeoutException("Deferred poll exceeded its request timeout");
            }
            HttpUriRequestBase exchange = cancellable(request);
            abort = deadlines.schedule(exchange::cancel, remainingNanos, TimeUnit.NANOSECONDS);
            // Rounded up to whole milliseconds, so neither fires before the deadline and reports it as a
            // connection failure. Their defaults would end a long readiness wait early.
            Timeout budget = Timeout.ofMilliseconds(TimeUnit.NANOSECONDS.toMillis(remainingNanos) + 1);
            context.setRequestConfig(RequestConfig.custom()
                    .setConnectionRequestTimeout(budget)
                    .setResponseTimeout(budget)
                    .build());
            return deferredHttpClient.execute(exchange, context, response -> {
                byte[] body;
                try {
                    body = readBounded(response.getEntity(), deadlineNanos, maxBodyBytes);
                } catch (IOException | RuntimeException unread) {
                    // A graceful close reads the rest of the body first, and a slow peer would hold the
                    // caller past its deadline. An immediate close discards the connection unread.
                    ((ModalCloseable) response).close(CloseMode.IMMEDIATE);
                    throw unread;
                }
                if (!isSuccessful(response.getCode())) {
                    throw errorResponse(response.getCode(), body);
                }
                return objectMapper.readValue(body, clazz);
            });
        } catch (IOException | CancellationException | IggyTimeoutException failure) {
            throw deferredFailure(Boolean.TRUE.equals(context.getAttribute(DEFERRED_ATTEMPTED)), failure);
        } finally {
            if (abort != null) {
                abort.cancel(false);
            }
            pendingDeferred.decrementAndGet();
        }
    }

    static IggyServerException deferredFailure(boolean possiblySent, Throwable failure) {
        var error = new IggyServerException(
                (possiblySent ? IggyErrorCode.TRANSIENT_NOT_COMMITTED : IggyErrorCode.TRANSIENT_NOT_ACCEPTED)
                        .getCode());
        error.initCause(failure);
        return error;
    }

    /** The client only lets a request that implements its cancellation contract be aborted mid-exchange. */
    private static HttpUriRequestBase cancellable(ClassicHttpRequest request) {
        HttpUriRequestBase cancellable;
        try {
            cancellable = new HttpUriRequestBase(request.getMethod(), request.getUri());
        } catch (URISyntaxException invalid) {
            throw new IggyInvalidArgumentException("Invalid deferred poll URI " + request.getRequestUri(), invalid);
        }
        cancellable.setHeaders(request.getHeaders());
        return cancellable;
    }

    /**
     * Reads the whole body under the caller's deadline. The deadline timer
     * aborts a read that blocks, and the check between reads refuses a body
     * that is still arriving after it. On failure the stream stays open, since
     * closing it would read the rest of the body: the caller discards the
     * connection instead.
     */
    private static byte[] readBounded(HttpEntity entity, long deadlineNanos, long maxBodyBytes) throws IOException {
        if (entity == null) {
            return new byte[0];
        }
        if (entity.getContentLength() > maxBodyBytes) {
            throw oversized(entity.getContentLength(), maxBodyBytes);
        }
        var body = new ByteArrayOutputStream();
        byte[] chunk = new byte[BODY_CHUNK_BYTES];
        InputStream content = entity.getContent();
        int read;
        while ((read = content.read(chunk)) >= 0) {
            if (Thread.interrupted()) {
                Thread.currentThread().interrupt();
                throw new InterruptedIOException("Interrupted while reading a deferred poll response");
            }
            if (deadlineNanos - System.nanoTime() <= 0) {
                throw new IggyTimeoutException("Deferred poll exceeded its request timeout while reading");
            }
            if (body.size() + read > maxBodyBytes) {
                throw oversized((long) body.size() + read, maxBodyBytes);
            }
            body.write(chunk, 0, read);
        }
        return body.toByteArray();
    }

    private static IggyMalformedResponseException oversized(long size, long limit) {
        return new IggyMalformedResponseException(
                "Deferred poll response body of " + size + " bytes exceeds the limit of " + limit);
    }

    /** A proxy or a failing peer can answer with a body that is no Iggy error, such as an HTML 503 page. */
    private IggyException errorResponse(int status, byte[] body) {
        String unreadable = "HTTP " + status + " response carries no Iggy error";
        JsonNode error;
        try {
            error = objectMapper.readTree(body);
        } catch (JacksonException parseFailure) {
            return new IggyMalformedResponseException(unreadable, parseFailure);
        }
        if (!(error instanceof ObjectNode)) {
            return new IggyMalformedResponseException(unreadable);
        }
        return IggyServerException.fromHttpResponse(
                textOrNull(error, "id"),
                textOrNull(error, "code"),
                textOrNull(error, "reason"),
                textOrNull(error, "field"));
    }

    private static String textOrNull(JsonNode node, String field) {
        return node.has(field) ? node.get(field).asString() : null;
    }

    @Override
    public void close() throws IOException {
        deadlines.shutdownNow();
        httpClient.close();
        deferredHttpClient.close();
    }

    private static void applyTlsCertificate(
            PoolingHttpClientConnectionManagerBuilder connectionManagerBuilder, Optional<File> tlsCertificate) {
        tlsCertificate.ifPresent(cert -> {
            try {
                var sslContext =
                        SSLContextBuilder.create().loadTrustMaterial(cert, null).build();
                connectionManagerBuilder.setTlsSocketStrategy(new DefaultClientTlsStrategy(sslContext));
            } catch (GeneralSecurityException | IOException e) {
                throw new IggyTlsException("Failed to configure TLS certificate", e);
            }
        });
    }

    void setToken(Optional<String> token) {
        this.token = token;
    }

    <T> T execute(ClassicHttpRequest request, Class<T> clazz) {
        return execute(request, objectMapper.constructType(clazz));
    }

    <T> T execute(ClassicHttpRequest request, TypeReference<T> typeReference) {
        return execute(request, objectMapper.constructType(typeReference));
    }

    private <T> T execute(ClassicHttpRequest request, JavaType type) {
        return executeRequest(request, response -> handleTypedResponse(response, type));
    }

    void execute(ClassicHttpRequest request) {
        executeRequest(request, response -> {
            handleErrorResponse(response);
            return "";
        });
    }

    public <T> Optional<T> executeWithOptionalResponse(ClassicHttpRequest request, Class<T> clazz) {
        return executeWithOptionalResponse(request, objectMapper.constructType(clazz));
    }

    private <T> Optional<T> executeWithOptionalResponse(ClassicHttpRequest request, JavaType type) {
        return executeRequest(request, response -> {
            if (response.getCode() == 404) {
                return Optional.empty();
            }
            return Optional.of(handleTypedResponse(response, type));
        });
    }

    String executeWithStringResponse(ClassicHttpRequest request) {
        return executeRequest(request, response -> {
            handleErrorResponse(response);
            return new String(response.getEntity().getContent().readAllBytes());
        });
    }

    private static CloseableHttpClient createDeferredHttpClient(
            Optional<Duration> connectionTimeout, Optional<File> tlsCertificate) {
        var connectionConfigBuilder = ConnectionConfig.custom();
        connectionTimeout.ifPresent(timeout -> connectionConfigBuilder.setConnectTimeout(Timeout.of(timeout)));
        var connectionManagerBuilder = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(connectionConfigBuilder.build())
                .setMaxConnTotal(MAX_DEFERRED_CONNECTIONS)
                .setMaxConnPerRoute(MAX_DEFERRED_CONNECTIONS);
        applyTlsCertificate(connectionManagerBuilder, tlsCertificate);
        return HttpClients.custom()
                .setConnectionManager(connectionManagerBuilder.build())
                .setRequestExecutor(new DeferredRequestExecutor())
                .disableAutomaticRetries()
                .evictIdleConnections(TimeValue.ofSeconds(30))
                .build();
    }

    private static ScheduledThreadPoolExecutor createDeadlineTimer() {
        var timer = new ScheduledThreadPoolExecutor(1, task -> {
            var thread = new Thread(task, "iggy-http-deferred-deadline");
            thread.setDaemon(true);
            return thread;
        });
        // Most polls end before their deadline, and a cancelled task would otherwise stay queued until then.
        timer.setRemoveOnCancelPolicy(true);
        return timer;
    }

    private <T> T executeRequest(ClassicHttpRequest request, HttpClientResponseHandler<T> responseHandler) {
        var context = HttpClientContext.create();
        try {
            return httpClient.execute(request, context, responseHandler);
        } catch (IggyServerException refusal) {
            throw settle(request, context, refusal);
        } catch (IOException e) {
            throw new IggyConnectionException("HTTP request failed", e);
        }
    }

    /**
     * HTTP has no request dedup: an automatic resend is a new request, so its refusal cannot tell
     * whether an earlier attempt committed. Such a refusal reports an unknown outcome, which
     * nothing refreshes or repeats.
     */
    private static IggyServerException settle(
            ClassicHttpRequest request, HttpContext context, IggyServerException refusal) {
        // Reads commit nothing, so only a write can leave an uncertain outcome behind.
        boolean uncertain =
                !Method.GET.isSame(request.getMethod()) && Boolean.TRUE.equals(context.getAttribute(UNCERTAIN_ATTEMPT));
        if (!uncertain || !SETTLED_REFUSALS.contains(refusal.getErrorCode())) {
            return refusal;
        }
        var unknownOutcome = new IggyServerException(IggyErrorCode.TRANSIENT_NOT_COMMITTED.getCode());
        unknownOutcome.initCause(refusal);
        return unknownOutcome;
    }

    ClassicHttpRequest prepareGetRequest(String path, NameValuePair... params) {
        return ClassicRequestBuilder.get(url + path)
                .setHeader(AUTHORIZATION, getBearerToken())
                .addParameters(params)
                .build();
    }

    ClassicHttpRequest preparePostRequest(String path, Object body) {
        var builder = ClassicRequestBuilder.post(url + path).setHeader(AUTHORIZATION, getBearerToken());
        return addRequestBody(builder, body);
    }

    ClassicHttpRequest preparePutRequest(String path, Object body) {
        var builder = ClassicRequestBuilder.put(url + path).setHeader(AUTHORIZATION, getBearerToken());
        return addRequestBody(builder, body);
    }

    ClassicHttpRequest prepareDeleteRequest(String path, NameValuePair... params) {
        return ClassicRequestBuilder.delete(url + path)
                .setHeader(AUTHORIZATION, getBearerToken())
                .addParameters(params)
                .build();
    }

    private ClassicHttpRequest addRequestBody(ClassicRequestBuilder requestBuilder, Object body) {
        var encodedBody = objectMapper.writeValueAsString(body);
        log.debug("Request body: {} bytes", encodedBody.length());
        return requestBuilder
                .setHeader("Content-Type", "application/json")
                .setEntity(encodedBody)
                .build();
    }

    private <T> T handleTypedResponse(ClassicHttpResponse response, JavaType type) throws IOException {
        handleErrorResponse(response);
        return objectMapper.readValue(response.getEntity().getContent(), type);
    }

    private void handleErrorResponse(ClassicHttpResponse response) throws IOException {
        if (!isSuccessful(response.getCode())) {
            HttpEntity entity = response.getEntity();
            throw errorResponse(
                    response.getCode(),
                    entity == null ? new byte[0] : entity.getContent().readAllBytes());
        }
    }

    private String getBearerToken() {
        return token.map(t -> "Bearer " + t).orElse("");
    }

    private static boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }

    private static final class DeferredRequestExecutor extends HttpRequestExecutor {
        @Override
        public ClassicHttpResponse execute(
                ClassicHttpRequest request,
                HttpClientConnection connection,
                HttpResponseInformationCallback callback,
                HttpContext context)
                throws IOException, HttpException {
            // The lease and connection already exist. A failed header write may still reach the server.
            context.setAttribute(DEFERRED_ATTEMPTED, Boolean.TRUE);
            return super.execute(request, connection, callback, context);
        }
    }

    /**
     * Resends what the default strategy resends: a 429 or a 503 answer to any request, and an I/O
     * failure of an idempotent one. Each resend marks the request uncertain unless the server
     * provably never took the attempt before it.
     */
    private static final class UncertainAttempts extends DefaultHttpRequestRetryStrategy {

        @Override
        public boolean retryRequest(HttpResponse response, int execCount, HttpContext context) {
            boolean retry = super.retryRequest(response, execCount, context);
            // Only an admission refusal proves the attempt was not taken. A 503 can mean either.
            if (retry && response.getCode() != HttpStatus.SC_TOO_MANY_REQUESTS) {
                context.setAttribute(UNCERTAIN_ATTEMPT, Boolean.TRUE);
            }
            return retry;
        }

        @Override
        public boolean retryRequest(HttpRequest request, IOException exception, int execCount, HttpContext context) {
            boolean retry = super.retryRequest(request, exception, execCount, context);
            // The default strategy never resends a failed connection, so the failed attempt may
            // have reached the server.
            if (retry) {
                context.setAttribute(UNCERTAIN_ATTEMPT, Boolean.TRUE);
            }
            return retry;
        }
    }
}
