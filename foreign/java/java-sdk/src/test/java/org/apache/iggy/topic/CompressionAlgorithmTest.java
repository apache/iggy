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

package org.apache.iggy.topic;

import org.apache.iggy.exception.IggyInvalidArgumentException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class CompressionAlgorithmTest {

    @ParameterizedTest
    @CsvSource({"1, None", "2, Gzip"})
    void fromCodeReturnsCorrectCompressionAlgorithmForValidCode(
            int code, CompressionAlgorithm expectedCompressionAlgorithm) {
        assertThat(CompressionAlgorithm.fromCode(code)).isEqualTo(expectedCompressionAlgorithm);
    }

    @Test
    void fromCodeThrowsIggyInvalidArgumentExceptionForInvalidCode() {
        assertThatThrownBy(() -> CompressionAlgorithm.fromCode((byte) 127))
                .isInstanceOf(IggyInvalidArgumentException.class);
    }

    @ParameterizedTest
    @CsvSource({"None, 1", "Gzip, 2"})
    void asCodeReturnsValue(CompressionAlgorithm compressionAlgorithm, int expectedCode) {
        assertThat(compressionAlgorithm.asCode()).isEqualTo(expectedCode);
    }
}
