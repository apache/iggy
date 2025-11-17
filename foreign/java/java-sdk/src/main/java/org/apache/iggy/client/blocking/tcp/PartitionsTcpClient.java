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

package org.apache.iggy.client.blocking.tcp;

import org.apache.iggy.client.blocking.PartitionsClient;
import org.apache.iggy.identifier.StreamId;
import org.apache.iggy.identifier.TopicId;

import static org.apache.iggy.client.blocking.tcp.BytesSerializer.toBytes;

class PartitionsTcpClient implements PartitionsClient {

    private final InternalTcpClient tcpClient;

    PartitionsTcpClient(InternalTcpClient tcpClient) {
        this.tcpClient = tcpClient;
    }

    @Override
    public void createPartitions(StreamId streamId, TopicId topicId, Long partitionsCount) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionsCount.intValue());
        tcpClient.send(CommandCode.Partition.CREATE, payload);
    }

    @Override
    public void deletePartitions(StreamId streamId, TopicId topicId, Long partitionsCount) {
        var payload = toBytes(streamId);
        payload.writeBytes(toBytes(topicId));
        payload.writeIntLE(partitionsCount.intValue());
        tcpClient.send(CommandCode.Partition.DELETE, payload);
    }
}
