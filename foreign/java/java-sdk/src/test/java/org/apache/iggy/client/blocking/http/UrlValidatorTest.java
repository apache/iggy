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

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class UrlValidatorTest {

    @ParameterizedTest
    @ValueSource(
            strings = {
                "http://localhost",
                "http://localhost:3000",
                "http://127.0.0.1:3000",
                "http://example.com",
                "http://example.com/path",
                "http://example.com:8080/path?query=value",
                "https://localhost",
                "https://localhost:3000",
                "https://127.0.0.1:3000",
                "https://example.com",
                "https://example.com/path",
                "https://example.com:443/path?query=value"
            })
    void shouldAcceptValidHttpUrls(String url) {
        assertThatCode(() -> UrlValidator.validateHttpUrl(url)).doesNotThrowAnyException();
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"   ", "\t", "\n"})
    void shouldRejectNullOrBlankUrls(String url) {
        assertThatThrownBy(() -> UrlValidator.validateHttpUrl(url))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessage("URL cannot be null or empty");
    }

    @ParameterizedTest
    @ValueSource(strings = {"ftp://example.com", "file:///path/to/file"})
    void shouldRejectNonHttpSchemes(String url) {
        assertThatThrownBy(() -> UrlValidator.validateHttpUrl(url))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessage("URL must start with http:// or https://");
    }

    @ParameterizedTest
    @ValueSource(strings = {"ws://example.com", "wss://example.com", "mailto:test@example.com"})
    void shouldRejectUnsupportedSchemes(String url) {
        // These schemes fail during URI.toURL() because Java has no protocol handler for them
        assertThatThrownBy(() -> UrlValidator.validateHttpUrl(url)).isInstanceOf(IggyInvalidArgumentException.class);
    }

    @Test
    void shouldRejectUrlWithoutProtocol() {
        assertThatThrownBy(() -> UrlValidator.validateHttpUrl("localhost:3000"))
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @ParameterizedTest
    @ValueSource(strings = {"http://", "https://", "http:// invalid", "http://[invalid"})
    void shouldRejectMalformedUrls(String url) {
        assertThatThrownBy(() -> UrlValidator.validateHttpUrl(url))
                .isInstanceOf(IggyInvalidArgumentException.class)
                .hasMessageStartingWith("Invalid URL:");
    }
}
