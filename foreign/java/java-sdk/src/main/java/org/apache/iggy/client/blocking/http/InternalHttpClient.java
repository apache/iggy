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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.core5.http.ClassicHttpRequest;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.NameValuePair;
import org.apache.hc.core5.http.io.HttpClientResponseHandler;
import org.apache.hc.core5.http.io.support.ClassicRequestBuilder;
import org.apache.iggy.client.blocking.http.error.IggyHttpError;
import org.apache.iggy.client.blocking.http.error.IggyHttpException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Optional;

final class InternalHttpClient {

    private static final Logger log = LoggerFactory.getLogger(InternalHttpClient.class);

    private static final String AUTHORIZATION = "Authorization";
    private final String url;
    private final ObjectMapper objectMapper = ObjectMapperFactory.getInstance();
    private Optional<String> token = Optional.empty();

    InternalHttpClient(String url) {
        this.url = url;
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

    private <T> T executeRequest(ClassicHttpRequest request, HttpClientResponseHandler<T> responseHandler) {
        try (var client = HttpClients.createDefault()) {
            return client.execute(request, responseHandler);
        } catch (IOException e) {
            throw new RuntimeException(e);
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
        try {
            var encodedBody = objectMapper.writeValueAsString(body);
            log.debug("Request body: {}", encodedBody);
            return requestBuilder
                    .setHeader("Content-Type", "application/json")
                    .setEntity(encodedBody)
                    .build();
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    private <T> T handleTypedResponse(ClassicHttpResponse response, JavaType type) throws IOException {
        handleErrorResponse(response);
        return objectMapper.readValue(response.getEntity().getContent(), type);
    }

    private void handleErrorResponse(ClassicHttpResponse response) throws IOException {
        if (!isSuccessful(response.getCode())) {
            var error = objectMapper.readValue(response.getEntity().getContent(), IggyHttpError.class);
            throw new IggyHttpException(error);
        }
    }

    private String getBearerToken() {
        return token.map(t -> "Bearer " + t).orElse("");
    }

    private static boolean isSuccessful(int statusCode) {
        return statusCode >= 200 && statusCode < 300;
    }
}
