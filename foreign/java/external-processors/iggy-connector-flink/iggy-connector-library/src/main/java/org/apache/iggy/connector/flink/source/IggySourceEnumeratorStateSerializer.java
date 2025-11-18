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

package org.apache.iggy.connector.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

/**
 * Serializer for IggySourceEnumeratorState for checkpointing.
 */
public class IggySourceEnumeratorStateSerializer implements SimpleVersionedSerializer<IggySourceEnumeratorState> {

    private static final int VERSION = 1;

    private final IggySourceSplitSerializer splitSerializer = new IggySourceSplitSerializer();

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IggySourceEnumeratorState state) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
                DataOutputStream out = new DataOutputStream(baos)) {

            // Write assigned splits
            Set<IggySourceSplit> assignedSplits = state.getAssignedSplits();
            out.writeInt(assignedSplits.size());
            for (IggySourceSplit split : assignedSplits) {
                byte[] splitBytes = splitSerializer.serialize(split);
                out.writeInt(splitBytes.length);
                out.write(splitBytes);
            }

            // Write discovered partitions
            Set<Integer> discoveredPartitions = state.getDiscoveredPartitions();
            out.writeInt(discoveredPartitions.size());
            for (Integer partitionId : discoveredPartitions) {
                out.writeInt(partitionId);
            }

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public IggySourceEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                DataInputStream in = new DataInputStream(bais)) {

            // Read assigned splits
            int splitsCount = in.readInt();
            Set<IggySourceSplit> assignedSplits = new HashSet<>();
            for (int i = 0; i < splitsCount; i++) {
                int splitBytesLength = in.readInt();
                byte[] splitBytes = new byte[splitBytesLength];
                in.readFully(splitBytes);
                IggySourceSplit split = splitSerializer.deserialize(splitSerializer.getVersion(), splitBytes);
                assignedSplits.add(split);
            }

            // Read discovered partitions
            int partitionsCount = in.readInt();
            Set<Integer> discoveredPartitions = new HashSet<>();
            for (int i = 0; i < partitionsCount; i++) {
                discoveredPartitions.add(in.readInt());
            }

            return new IggySourceEnumeratorState(assignedSplits, discoveredPartitions);
        }
    }
}
