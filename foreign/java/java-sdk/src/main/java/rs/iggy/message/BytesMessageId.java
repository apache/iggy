package rs.iggy.message;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import java.math.BigInteger;
import java.util.Arrays;

public class BytesMessageId implements MessageId {

    private final byte[] value;

    public BytesMessageId(byte[] value) {
        if (value.length != 16) {
            throw new IllegalArgumentException("Message id must have 16 bytes");
        }
        this.value = value;
    }

    @Override
    public BigInteger toBigInteger() {
        return new BigInteger(1, value);
    }

    @Override
    public ByteBuf toBytes() {
        return Unpooled.wrappedBuffer(value);
    }

    @Override
    public String toString() {
        return Arrays.toString(value);
    }

}
