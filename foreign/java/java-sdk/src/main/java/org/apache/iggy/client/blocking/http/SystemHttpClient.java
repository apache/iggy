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

import org.apache.iggy.client.blocking.SystemClient;
import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.exception.IggyOperationNotSupportedException;
import org.apache.iggy.message.HeaderKind;
import org.apache.iggy.message.HeaderValue;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.system.Stats;
import tools.jackson.core.type.TypeReference;

import java.util.List;
import java.util.Locale;

class SystemHttpClient implements SystemClient {

    private static final String STATS = "/stats";
    private static final String CLUSTER_METADATA = "/cluster/metadata";
    private static final String CLIENTS = "/clients";
    private static final String PING = "/ping";
    private static final String OPTIONS = "/options";
    private final InternalHttpClient httpClient;

    public SystemHttpClient(InternalHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public Stats getStats() {
        var request = httpClient.prepareGetRequest(STATS);
        return httpClient.execute(request, Stats.class);
    }

    @Override
    public ClusterMetadata getClusterMetadata() {
        var request = httpClient.prepareGetRequest(CLUSTER_METADATA);
        return httpClient.execute(request, ClusterMetadata.class);
    }

    @Override
    public List<OptionSpec> describeOptions(OptionsScope scope) {
        var request = httpClient.prepareGetRequest(OPTIONS + "/" + scope.name().toLowerCase(Locale.ROOT));
        List<HttpOptionSpec> specs = httpClient.execute(request, new TypeReference<>() {});
        return specs.stream().map(HttpOptionSpec::toOptionSpec).toList();
    }

    @Override
    public ClientInfoDetails getMe() {
        throw new IggyOperationNotSupportedException("getMe", "HTTP");
    }

    @Override
    public ClientInfoDetails getClient(Long clientId) {
        var request = httpClient.prepareGetRequest(CLIENTS + "/" + clientId);
        return httpClient.execute(request, ClientInfoDetails.class);
    }

    @Override
    public List<ClientInfo> getClients() {
        var request = httpClient.prepareGetRequest(CLIENTS);
        return httpClient.execute(request, new TypeReference<>() {});
    }

    @Override
    public String ping() {
        var request = httpClient.prepareGetRequest(PING);
        return httpClient.executeWithStringResponse(request);
    }

    /**
     * The REST shape of a catalog entry: the kind arrives as its lowercase name and the default as a
     * JSON array, where the binary transport sends a kind code and raw bytes. Mapping it here keeps
     * {@link OptionSpec} the one shape a caller sees on either transport.
     */
    record HttpOptionSpec(String key, HeaderKind kind, byte[] defaultValue, String description) {
        OptionSpec toOptionSpec() {
            return new OptionSpec(key, new HeaderValue(kind, defaultValue), description);
        }
    }
}
