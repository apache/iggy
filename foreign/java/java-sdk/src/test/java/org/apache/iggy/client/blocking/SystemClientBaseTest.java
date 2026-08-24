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

package org.apache.iggy.client.blocking;

import org.apache.iggy.cluster.ClusterNodeRole;
import org.apache.iggy.cluster.ClusterNodeStatus;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;

public abstract class SystemClientBaseTest extends IntegrationTest {

    protected SystemClient systemClient;

    @BeforeEach
    void beforeEachBase() {
        systemClient = client.system();

        login();
    }

    @Test
    void shouldDescribeTheTopicOptionCatalog() {
        // when
        var specs = systemClient.describeOptions(OptionsScope.Topic);

        // then
        var byKey = specs.stream().collect(Collectors.toMap(OptionSpec::key, spec -> spec));
        assertThat(byKey).containsKeys("segment_size", "enforce_fsync");
        var segmentSize = byKey.get("segment_size");
        assertThat(segmentSize.defaultValue().kind()).isEqualTo(HeaderKind.Uint64);
        assertThat(segmentSize.defaultValue().value()).isNotEmpty();
        assertThat(segmentSize.description()).isNotBlank();
    }

    @Test
    void shouldDescribeEmptyCatalogsForScopesWithoutKeys() {
        // Streams and users have no catalog keys yet, so the scope answers with
        // an empty list rather than an error.
        assertThat(systemClient.describeOptions(OptionsScope.Stream)).isEmpty();
        assertThat(systemClient.describeOptions(OptionsScope.User)).isEmpty();
    }

    @Test
    void shouldGetStats() {
        // when
        var stats = systemClient.getStats();

        // then
        assertThat(stats).isNotNull();
        assertThat(stats.processId()).isNotNull();
        assertThat(stats.hostname()).isNotEmpty();
        assertThat(stats.threadsCount()).isNotNull();
        assertThat(stats.freeDiskSpace()).isNotNull();
        assertThat(stats.totalDiskSpace()).isNotNull();
    }

    @Test
    void shouldGetClusterMetadataForSingleNode() {
        // when
        var metadata = systemClient.getClusterMetadata();

        // then
        assertThat(metadata).isNotNull();
        // The server runs standalone (cluster disabled), so it synthesizes
        // itself as the sole leader of a single-node roster.
        assertThat(metadata.name()).isEqualTo("single-node");
        assertThat(metadata.nodes()).hasSize(1);
        var node = metadata.nodes().get(0);
        assertThat(node.name()).isEqualTo("iggy-node");
        assertThat(node.role()).isEqualTo(ClusterNodeRole.Leader);
        assertThat(node.status()).isEqualTo(ClusterNodeStatus.Healthy);
        assertThat(node.endpoints().tcp()).isPositive();
    }

    @Test
    void shouldPing() {
        // when
        var result = systemClient.ping();

        // then
        assertThat(result).isEqualTo("pong");
    }

    @Test
    void shouldPingWithoutAuthentication() {
        // given — fresh client, connected but not logged in
        var unauthenticatedClient = getClient();

        // when
        var result = unauthenticatedClient.system().ping();

        // then
        assertThat(result).isEqualTo("pong");
    }
}
