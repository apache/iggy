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

package org.apache.iggy.exception;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IggyTimeoutExceptionTest {
    @Test
    void constructorWithNoArgsCreatesExpectedIggyTimeoutException() {
        var exception = new IggyTimeoutException();

        assertThat(exception.getMessage()).isEqualToIgnoringCase("Operation timed out");
    }

    @Test
    void constructorWithMessageCreatesExpectedIggyTimeoutException() {
        var exception = new IggyTimeoutException("message");

        assertThat(exception.getMessage()).isEqualTo("message");
        assertThat(exception.getCause()).isNull();
    }

    @Test
    void constructorWithMessageAndCauseCreatesExpectedIggyTimeoutException() {
        var cause = new RuntimeException("cause");
        var exception = new IggyTimeoutException("message", cause);

        assertThat(exception.getMessage()).isEqualTo("message");
        assertThat(exception.getCause()).isSameAs(cause);
    }
}
