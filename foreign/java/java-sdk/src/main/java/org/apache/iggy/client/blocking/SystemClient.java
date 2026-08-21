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

import org.apache.iggy.cluster.ClusterMetadata;
import org.apache.iggy.system.ClientInfo;
import org.apache.iggy.system.ClientInfoDetails;
import org.apache.iggy.system.OptionSpec;
import org.apache.iggy.system.OptionsScope;
import org.apache.iggy.system.Stats;

import java.util.List;

public interface SystemClient {

    Stats getStats();

    ClusterMetadata getClusterMetadata();

    /**
     * Describes the option catalog for a resource scope: every key its create command accepts, with
     * the kind, default and bounds of each.
     *
     * <p>A key outside the catalog is refused at create and the binary transports carry only the
     * error code back, so this is the only way to learn which keys a server knows. A scope with no
     * keys yet returns an empty list.
     */
    List<OptionSpec> describeOptions(OptionsScope scope);

    ClientInfoDetails getMe();

    ClientInfoDetails getClient(Long clientId);

    List<ClientInfo> getClients();

    String ping();
}
