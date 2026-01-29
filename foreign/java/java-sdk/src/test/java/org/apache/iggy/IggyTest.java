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

package org.apache.iggy;

import org.apache.iggy.builder.HttpClientBuilder;
import org.apache.iggy.builder.TcpClientBuilder;
import org.apache.iggy.client.async.tcp.AsyncIggyTcpClientBuilder;
import org.apache.iggy.client.blocking.http.IggyHttpClientBuilder;
import org.apache.iggy.client.blocking.tcp.IggyTcpClientBuilder;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class IggyTest {

    @Test
    void tcpBuilderReturnsCorrectTypes() {
        TcpClientBuilder tcpBuilder = Iggy.tcpClientBuilder();
        assertThat(tcpBuilder).isNotNull();

        IggyTcpClientBuilder blockingBuilder = tcpBuilder.blocking();
        assertThat(blockingBuilder).isNotNull();

        AsyncIggyTcpClientBuilder asyncBuilder = tcpBuilder.async();
        assertThat(asyncBuilder).isNotNull();
    }

    @Test
    void httpBuilderReturnsCorrectTypes() {
        HttpClientBuilder httpBuilder = Iggy.httpClientBuilder();
        assertThat(httpBuilder).isNotNull();

        IggyHttpClientBuilder blockingBuilder = httpBuilder.blocking();
        assertThat(blockingBuilder).isNotNull();
    }

    @Test
    void versionMethodsReturnValues() {
        String version = Iggy.version();
        assertThat(version).isNotNull();
        assertThat(version).isNotEqualTo("unknown");

        IggyVersion versionInfo = Iggy.versionInfo();
        assertThat(versionInfo).isNotNull();
        assertThat(versionInfo.getVersion()).isNotNull().isNotEqualTo("unknown");
        assertThat(versionInfo.getBuildTime()).isNotNull().isNotEqualTo("unknown");
        assertThat(versionInfo.getGitCommit()).isNotNull().isNotEqualTo("unknown");
        assertThat(versionInfo.getUserAgent()).startsWith("iggy-java-sdk/");
    }

    @Test
    void tcpBlockingBuilderHasFluentApi() {
        IggyTcpClientBuilder builder = Iggy.tcpClientBuilder().blocking();

        // Verify fluent API returns same builder
        assertThat(builder.host("localhost")).isSameAs(builder);
        assertThat(builder.port(8090)).isSameAs(builder);
        assertThat(builder.enableTls()).isSameAs(builder);
        assertThat(builder.tls(false)).isSameAs(builder);
    }

    @Test
    void tcpAsyncBuilderHasFluentApi() {
        AsyncIggyTcpClientBuilder builder = Iggy.tcpClientBuilder().async();

        // Verify fluent API returns same builder
        assertThat(builder.host("localhost")).isSameAs(builder);
        assertThat(builder.port(8090)).isSameAs(builder);
        assertThat(builder.enableTls()).isSameAs(builder);
        assertThat(builder.tls(false)).isSameAs(builder);
    }

    @Test
    void httpBlockingBuilderHasFluentApi() {
        IggyHttpClientBuilder builder = Iggy.httpClientBuilder().blocking();

        // Verify fluent API returns same builder
        assertThat(builder.host("localhost")).isSameAs(builder);
        assertThat(builder.port(3000)).isSameAs(builder);
        assertThat(builder.url("http://localhost:3000")).isSameAs(builder);
        assertThat(builder.enableTls()).isSameAs(builder);
        assertThat(builder.tls(false)).isSameAs(builder);
    }
}
