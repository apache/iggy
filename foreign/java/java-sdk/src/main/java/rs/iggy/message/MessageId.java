package rs.iggy.message;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.netty.buffer.ByteBuf;
import java.math.BigInteger;

public interface MessageId {

    static MessageId serverGenerated() {
        return BigIntegerMessageId.defaultId();
    }

    @JsonCreator
    static MessageId from(BigInteger bigInteger) {
        return new BigIntegerMessageId(bigInteger);
    }

    @JsonValue
    BigInteger toBigInteger();

    ByteBuf toBytes();

}
