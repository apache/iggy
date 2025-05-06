package rs.iggy.user;

public record TopicPermissions(
        boolean manageTopic,
        boolean readTopic,
        boolean pollMessages,
        boolean sendMessages
) {
}
