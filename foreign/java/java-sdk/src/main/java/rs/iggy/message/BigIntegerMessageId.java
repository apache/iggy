package rs.iggy.message;

import io.netty.buffer.ByteBuf;
import rs.iggy.clients.blocking.tcp.BytesSerializer;
import java.math.BigInteger;

public class BigIntegerMessageId implements MessageId {

    private static final BigIntegerMessageId DEFAULT_ID = new BigIntegerMessageId(BigInteger.ZERO);
    private final BigInteger value;

    public BigIntegerMessageId(BigInteger value) {
        this.value = value;
    }

    public static BigIntegerMessageId defaultId() {
        return DEFAULT_ID;
    }

    @Override
    public BigInteger toBigInteger() {
        return value;
    }

    public ByteBuf toBytes() {
        return BytesSerializer.toBytesAsU128(value);
    }

    @Override
    public String toString() {
        return value.toString();
    }

}
