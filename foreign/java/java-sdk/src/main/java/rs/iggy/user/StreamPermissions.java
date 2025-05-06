package rs.iggy.user;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import java.util.Map;

public record StreamPermissions(
        boolean manageStream,
        boolean readStream,
        boolean manageTopics,
        boolean readTopics,
        boolean pollMessages,
        boolean sendMessages,
        @JsonSetter(nulls = Nulls.AS_EMPTY)
        Map<Long, TopicPermissions> topics
) {
}
