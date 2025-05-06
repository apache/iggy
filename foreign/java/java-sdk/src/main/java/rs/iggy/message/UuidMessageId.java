package rs.iggy.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.UUID;

public class UuidMessageId implements MessageId {

    private final UUID value;

    public UuidMessageId(UUID value) {
        this.value = value;
    }

    @Override
    public BigInteger toBigInteger() {
        ByteBuffer buffer = ByteBuffer.wrap(new byte[16]);
        buffer.putLong(value.getMostSignificantBits());
        buffer.putLong(value.getLeastSignificantBits());
        return new BigInteger(1, buffer.array());
    }

    public ByteBuf toBytes() {
        var buffer = Unpooled.buffer(16, 16);
        buffer.writeLongLE(value.getLeastSignificantBits());
        buffer.writeLongLE(value.getMostSignificantBits());
        return buffer;
    }

    @Override
    public String toString() {
        return value.toString();
    }

}
