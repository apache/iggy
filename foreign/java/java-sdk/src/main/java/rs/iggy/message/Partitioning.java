package rs.iggy.message;

import org.apache.commons.lang3.ArrayUtils;
import java.nio.ByteBuffer;

public record Partitioning(
        PartitioningKind kind,
        byte[] value
) {

    public static Partitioning balanced() {
        return new Partitioning(PartitioningKind.Balanced, new byte[]{});
    }

    public static Partitioning partitionId(Long id) {
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(id.intValue());
        byte[] partitionId = buffer.array();
        ArrayUtils.reverse(partitionId);
        return new Partitioning(PartitioningKind.PartitionId, partitionId);
    }

    public static Partitioning messagesKey(String key) {
        if (key == null || key.isBlank() || key.length() > 255) {
            throw new IllegalArgumentException("Key must be non-empty and less than 255 characters long");
        }
        return new Partitioning(PartitioningKind.MessagesKey, key.getBytes());
    }
}
