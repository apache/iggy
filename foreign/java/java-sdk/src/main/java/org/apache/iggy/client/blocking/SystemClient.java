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
import org.apache.iggy.system.MetadataChanges;
import org.apache.iggy.system.MetadataChangesQuery;
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

    /**
     * Reads one page of the node's metadata change log, or of its current state.
     *
     * <p>Every committed metadata operation that changes public state leaves records such as a stream created, a
     * topic renamed or a session closed. A mirror keeps a cursor, asks for the records after it and stores the
     * returned {@link MetadataChanges#next()}. On expired or reset it rebuilds from current state pages and follows
     * the changes after the anchor they return. Requires root or the global manage servers permission, and the query
     * is checked before anything is sent.
     *
     * @throws org.apache.iggy.exception.IggyValidationException invalid command for a query the rules refuse
     * @throws org.apache.iggy.exception.IggyServerException unauthorized without the manage servers permission
     */
    MetadataChanges getMetadataChanges(MetadataChangesQuery query);

    ClientInfoDetails getMe();

    ClientInfoDetails getClient(Long clientId);

    List<ClientInfo> getClients();

    String ping();
}
