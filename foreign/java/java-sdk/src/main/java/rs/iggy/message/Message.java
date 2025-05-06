package rs.iggy.message;

import java.util.Map;
import java.util.Optional;

public record Message(
        MessageId id,
        byte[] payload,
        Optional<Map<String, HeaderValue>> headers
) {

    public static Message of(String payload) {
        return new Message(MessageId.serverGenerated(), payload.getBytes(), Optional.empty());
    }

}
