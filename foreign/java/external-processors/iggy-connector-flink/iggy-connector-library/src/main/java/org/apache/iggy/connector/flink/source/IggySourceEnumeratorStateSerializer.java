package org.apache.iggy.connector.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.util.HashSet;
import java.util.Set;

/**
 * Serializer for IggySourceEnumeratorState for checkpointing.
 */
public class IggySourceEnumeratorStateSerializer
        implements SimpleVersionedSerializer<IggySourceEnumeratorState> {

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
                IggySourceSplit split = splitSerializer.deserialize(
                        splitSerializer.getVersion(), splitBytes);
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
