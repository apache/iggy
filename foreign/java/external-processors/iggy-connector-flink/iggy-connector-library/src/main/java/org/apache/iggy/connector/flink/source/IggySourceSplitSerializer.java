package org.apache.iggy.connector.flink.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;

import java.io.*;
import java.nio.charset.StandardCharsets;

/**
 * Serializer for IggySourceSplit for checkpointing.
 */
public class IggySourceSplitSerializer implements SimpleVersionedSerializer<IggySourceSplit> {

    private static final int VERSION = 1;

    @Override
    public int getVersion() {
        return VERSION;
    }

    @Override
    public byte[] serialize(IggySourceSplit split) throws IOException {
        try (ByteArrayOutputStream baos = new ByteArrayOutputStream();
             DataOutputStream out = new DataOutputStream(baos)) {

            // Write streamId
            byte[] streamIdBytes = split.getStreamId().getBytes(StandardCharsets.UTF_8);
            out.writeInt(streamIdBytes.length);
            out.write(streamIdBytes);

            // Write topicId
            byte[] topicIdBytes = split.getTopicId().getBytes(StandardCharsets.UTF_8);
            out.writeInt(topicIdBytes.length);
            out.write(topicIdBytes);

            // Write partitionId
            out.writeInt(split.getPartitionId());

            // Write startOffset
            out.writeLong(split.getStartOffset());

            // Write currentOffset
            out.writeLong(split.getCurrentOffset());

            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public IggySourceSplit deserialize(int version, byte[] serialized) throws IOException {
        if (version != VERSION) {
            throw new IOException("Unsupported version: " + version);
        }

        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
             DataInputStream in = new DataInputStream(bais)) {

            // Read streamId
            int streamIdLength = in.readInt();
            byte[] streamIdBytes = new byte[streamIdLength];
            in.readFully(streamIdBytes);
            String streamId = new String(streamIdBytes, StandardCharsets.UTF_8);

            // Read topicId
            int topicIdLength = in.readInt();
            byte[] topicIdBytes = new byte[topicIdLength];
            in.readFully(topicIdBytes);
            String topicId = new String(topicIdBytes, StandardCharsets.UTF_8);

            // Read partitionId
            int partitionId = in.readInt();

            // Read startOffset
            long startOffset = in.readLong();

            // Read currentOffset
            long currentOffset = in.readLong();

            return new IggySourceSplit(streamId, topicId, partitionId, startOffset, currentOffset);
        }
    }
}
