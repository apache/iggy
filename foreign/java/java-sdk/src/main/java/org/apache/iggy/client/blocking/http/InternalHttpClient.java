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

import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.protocol.HttpClientContext;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.hc.core5.ssl.SSLContextBuilder;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;
import org.apache.iggy.exception.IggyClientException;
import org.apache.iggy.exception.IggyConnectionException;
import org.apache.iggy.exception.IggyMalformedResponseException;
import org.apache.iggy.exception.IggyServerException;
import org.apache.iggy.exception.IggyTimeoutException;
import org.apache.iggy.exception.IggyTlsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import tools.jackson.core.type.TypeReference;
import tools.jackson.databind.JavaType;
import tools.jackson.databind.ObjectMapper;
import tools.jackson.databind.node.ObjectNode;

import java.io.ByteArrayOutputStream;
import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

final class InternalHttpClient implements Closeable {

    private static final Logger log = LoggerFactory.getLogger(InternalHttpClient.class);

    private static final String AUTHORIZATION = "Authorization";

    /**
     * Deferred requests occupy their connection for the whole readiness wait, so
     * they get their own pool and can never starve control traffic. The bound
     * matches the binary client's deferred lease count.
     */
    private static final int MAX_DEFERRED_CONNECTIONS = 16;

    /** Mirrors the binary client's pending-poll admission cap. */
    private static final int MAX_PENDING_DEFERRED_REQUESTS = 4096;

    private static final int BODY_CHUNK_BYTES = 8 * 1024;

    private final String url;
    private final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
    private final CloseableHttpClient httpClient;
    private final CloseableHttpClient deferredHttpClient;
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
                .build();
    }

    /**
     * A deferred poll declares its own total budget, so the library's response
     * timeout is set from the caller's remaining time rather than a shared
     * setting, and automatic retries are off: the server answers both
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
            throw new IggyClientException("Too many pending deferred HTTP polls");
        }
        try {
            long remainingNanos = deadlineNanos - System.nanoTime();
            if (remainingNanos <= 0) {
                throw new IggyTimeoutException("Deferred poll exceeded its request timeout");
            }
            var context = HttpClientContext.create();
            context.setRequestConfig(RequestConfig.custom()
                    .setConnectionRequestTimeout(Timeout.ofNanoseconds(remainingNanos))
                    .setResponseTimeout(Timeout.ofNanoseconds(remainingNanos))
                    .build());
            return deferredHttpClient.execute(request, context, response -> {
                byte[] body = readBounded(response.getEntity(), deadlineNanos, maxBodyBytes);
                if (!isSuccessful(response.getCode())) {
                    throw toServerException(body);
                }
                return objectMapper.readValue(body, clazz);
            });
        } catch (IOException e) {
            throw new IggyConnectionException("Deferred HTTP request failed", e);
        } finally {
            pendingDeferred.decrementAndGet();
        }
    }

    /**
     * Reads the whole body under the caller's deadline. A response timeout only
     * bounds inactivity, so a peer that keeps trickling bytes would otherwise
     * hold the caller past its budget. Throwing here aborts the exchange, which
     * discards the connection and frees its pool slot.
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
        try (InputStream content = entity.getContent()) {
            int read;
            while ((read = content.read(chunk)) >= 0) {
                if (Thread.interrupted()) {
                    Thread.currentThread().interrupt();
                    throw new IggyClientException("Interrupted while reading a deferred poll response");
                }
                if (deadlineNanos - System.nanoTime() <= 0) {
                    throw new IggyTimeoutException("Deferred poll exceeded its request timeout while reading");
                }
                if (body.size() + read > maxBodyBytes) {
                    throw oversized((long) body.size() + read, maxBodyBytes);
                }
                body.write(chunk, 0, read);
            }
        }
        return body.toByteArray();
    }

    private static IggyMalformedResponseException oversized(long size, long limit) {
        return new IggyMalformedResponseException(
                "Deferred poll response body of " + size + " bytes exceeds the limit of " + limit);
    }

    private IggyServerException toServerException(byte[] body) {
        ObjectNode errorNode = objectMapper.readValue(body, ObjectNode.class);
        return IggyServerException.fromHttpResponse(
                textOrNull(errorNode, "id"),
                textOrNull(errorNode, "code"),
                textOrNull(errorNode, "reason"),
                textOrNull(errorNode, "field"));
    }

    private static String textOrNull(ObjectNode node, String field) {
        return node.has(field) ? node.get(field).asString() : null;
    }

    @Override
    public void close() throws IOException {
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
                .disableAutomaticRetries()
                .evictIdleConnections(TimeValue.ofSeconds(30))
                .build();
    }

    private <T> T executeRequest(ClassicHttpRequest request, HttpClientResponseHandler<T> responseHandler) {
        try {
            return httpClient.execute(request, responseHandler);
        } catch (IOException e) {
            throw new IggyConnectionException("HTTP request failed", e);
        }
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
        log.debug("Request body: {}", encodedBody);
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
            var errorNode = objectMapper.readValue(response.getEntity().getContent(), ObjectNode.class);
            String id = errorNode.has("id") ? errorNode.get("id").asString() : null;
            String code = errorNode.has("code") ? errorNode.get("code").asString() : null;
            String reason = errorNode.has("reason") ? errorNode.get("reason").asString() : null;
            String field = errorNode.has("field") ? errorNode.get("field").asString() : null;
            throw IggyServerException.fromHttpResponse(id, code, reason, field);
        }
    }

    private String getBearerToken() {
        return token.map(t -> "Bearer " + t).orElse("");
    }

    private static boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
