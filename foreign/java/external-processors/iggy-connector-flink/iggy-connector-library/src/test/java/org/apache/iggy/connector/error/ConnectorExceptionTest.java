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

package org.apache.iggy.connector.error;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectorExceptionTest {

    @Test
    void shouldCreateExceptionWithMessage() {
        ConnectorException exception = new ConnectorException("Test message");

        assertThat(exception.getMessage()).isEqualTo("Test message");
        assertThat(exception.getErrorCode()).isEqualTo(ConnectorException.ErrorCode.UNKNOWN);
        assertThat(exception.isRetryable()).isFalse();
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void shouldCreateExceptionWithMessageAndCause() {
        Throwable cause = new RuntimeException("Root cause");
        ConnectorException exception = new ConnectorException("Test message", cause);

        assertThat(exception.getMessage()).isEqualTo("Test message");
        assertThat(exception.getCause()).isEqualTo(cause);
        assertThat(exception.getErrorCode()).isEqualTo(ConnectorException.ErrorCode.UNKNOWN);
        assertThat(exception.isRetryable()).isFalse();
    }

    @Test
    void shouldCreateExceptionWithMessageErrorCodeAndRetryable() {
        ConnectorException exception =
                new ConnectorException("Connection failed", ConnectorException.ErrorCode.CONNECTION_FAILED, true);

        assertThat(exception.getMessage()).isEqualTo("Connection failed");
        assertThat(exception.getErrorCode()).isEqualTo(ConnectorException.ErrorCode.CONNECTION_FAILED);
        assertThat(exception.isRetryable()).isTrue();
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void shouldCreateExceptionWithAllParameters() {
        Throwable cause = new RuntimeException("Root cause");
        ConnectorException exception = new ConnectorException(
                "Connection failed", cause, ConnectorException.ErrorCode.CONNECTION_FAILED, true);

        assertThat(exception.getMessage()).isEqualTo("Connection failed");
        assertThat(exception.getCause()).isEqualTo(cause);
        assertThat(exception.getErrorCode()).isEqualTo(ConnectorException.ErrorCode.CONNECTION_FAILED);
        assertThat(exception.isRetryable()).isTrue();
    }

    @Test
    void shouldSupportAllErrorCodes() {
        assertThat(ConnectorException.ErrorCode.UNKNOWN).isNotNull();
        assertThat(ConnectorException.ErrorCode.CONNECTION_FAILED).isNotNull();
        assertThat(ConnectorException.ErrorCode.AUTHENTICATION_FAILED).isNotNull();
        assertThat(ConnectorException.ErrorCode.RESOURCE_NOT_FOUND).isNotNull();
        assertThat(ConnectorException.ErrorCode.SERIALIZATION_ERROR).isNotNull();
        assertThat(ConnectorException.ErrorCode.OFFSET_ERROR).isNotNull();
        assertThat(ConnectorException.ErrorCode.PARTITION_ERROR).isNotNull();
        assertThat(ConnectorException.ErrorCode.CONFIGURATION_ERROR).isNotNull();
        assertThat(ConnectorException.ErrorCode.TIMEOUT).isNotNull();
        assertThat(ConnectorException.ErrorCode.RESOURCE_EXHAUSTED).isNotNull();
    }

    @Test
    void shouldMarkConnectionErrorsAsRetryable() {
        ConnectorException exception =
                new ConnectorException("Connection failed", ConnectorException.ErrorCode.CONNECTION_FAILED, true);

        assertThat(exception.isRetryable()).isTrue();
    }

    @Test
    void shouldMarkAuthenticationErrorsAsNonRetryable() {
        ConnectorException exception = new ConnectorException(
                "Authentication failed", ConnectorException.ErrorCode.AUTHENTICATION_FAILED, false);

        assertThat(exception.isRetryable()).isFalse();
    }

    @Test
    void shouldMarkSerializationErrorsAsNonRetryable() {
        ConnectorException exception =
                new ConnectorException("Serialization failed", ConnectorException.ErrorCode.SERIALIZATION_ERROR, false);

        assertThat(exception.isRetryable()).isFalse();
    }

    @Test
    void shouldMarkTimeoutErrorsAsRetryable() {
        ConnectorException exception =
                new ConnectorException("Operation timed out", ConnectorException.ErrorCode.TIMEOUT, true);

        assertThat(exception.isRetryable()).isTrue();
    }

    @Test
    void shouldMarkConfigurationErrorsAsNonRetryable() {
        ConnectorException exception =
                new ConnectorException("Configuration error", ConnectorException.ErrorCode.CONFIGURATION_ERROR, false);

        assertThat(exception.isRetryable()).isFalse();
    }

    @Test
    void shouldBeInstanceOfRuntimeException() {
        ConnectorException exception = new ConnectorException("Test");
        assertThat(exception).isInstanceOf(RuntimeException.class);
    }

    @Test
    void shouldBeSerializable() {
        ConnectorException exception = new ConnectorException("Test");
        assertThat(exception).isInstanceOf(java.io.Serializable.class);
    }

    @Test
    void shouldPreserveStackTrace() {
        Throwable cause = new RuntimeException("Root cause");
        ConnectorException exception = new ConnectorException("Wrapper", cause);

        assertThat(exception.getStackTrace()).isNotEmpty();
        assertThat(exception.getCause().getStackTrace()).isNotEmpty();
    }
}
