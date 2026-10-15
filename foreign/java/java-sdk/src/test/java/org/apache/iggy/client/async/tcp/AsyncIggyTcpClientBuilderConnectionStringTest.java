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

package org.apache.iggy.client.async.tcp;

import org.junit.jupiter.api.Test;

import java.io.File;

import static org.assertj.core.api.Assertions.assertThat;

class AsyncIggyTcpClientBuilderConnectionStringTest {

    @Test
    void shouldKeepEarlierTlsWhenConnectionStringDoesNotSetIt() {
        AsyncIggyTcpClientBuilder builder =
                AsyncIggyTcpClient.builder().enableTls().fromConnectionString("iggy://iggy:iggy@localhost:8090");

        assertThat(builder.isTlsEnabled()).isTrue();
    }

    @Test
    void shouldKeepEarlierCertificateWhenConnectionStringDoesNotSetIt() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .enableTls()
                .tlsCertificate("/certs/ca.pem")
                .fromConnectionString("iggy://iggy:iggy@localhost:8090");

        assertThat(builder.tlsCertificateFile()).isEqualTo(new File("/certs/ca.pem"));
    }

    @Test
    void shouldApplyTlsFromConnectionString() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .fromConnectionString("iggy://iggy:iggy@localhost:8090?tls=true&tls_ca_file=/certs/ca.pem");

        assertThat(builder.isTlsEnabled()).isTrue();
        assertThat(builder.tlsCertificateFile()).isEqualTo(new File("/certs/ca.pem"));
    }

    @Test
    void shouldDisableTlsWhenConnectionStringSetsTlsFalse() {
        AsyncIggyTcpClientBuilder builder = AsyncIggyTcpClient.builder()
                .enableTls()
                .fromConnectionString("iggy://iggy:iggy@localhost:8090?tls=false");

        assertThat(builder.isTlsEnabled()).isFalse();
    }
}
