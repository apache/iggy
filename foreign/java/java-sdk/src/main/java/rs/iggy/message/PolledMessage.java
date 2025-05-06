package rs.iggy.message;

import java.math.BigInteger;
import java.util.Map;
import java.util.Optional;

public record PolledMessage(
        BigInteger offset,
        MessageState state,
        BigInteger timestamp,
        MessageId id,
        Long checksum,
        Optional<Map<String, HeaderValue>> headers,
        byte[] payload
) {
}
