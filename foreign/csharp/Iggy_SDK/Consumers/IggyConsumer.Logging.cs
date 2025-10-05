using Apache.Iggy.Enums;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Consumers;

public partial class IggyConsumer
{
    // Information logs
    [LoggerMessage(
        EventId = 100,
        Level = LogLevel.Information,
        Message = "Creating consumer group '{GroupName}' for stream {StreamId}, topic {TopicId}")]
    private partial void LogCreatingConsumerGroup(string groupName, Identifier streamId, Identifier topicId);

    [LoggerMessage(
        EventId = 101,
        Level = LogLevel.Information,
        Message = "Successfully created consumer group '{GroupName}'")]
    private partial void LogConsumerGroupCreated(string groupName);

    [LoggerMessage(
        EventId = 102,
        Level = LogLevel.Information,
        Message = "Joining consumer group '{GroupName}' for stream {StreamId}, topic {TopicId}")]
    private partial void LogJoiningConsumerGroup(string groupName, Identifier streamId, Identifier topicId);

    [LoggerMessage(
        EventId = 103,
        Level = LogLevel.Information,
        Message = "Successfully joined consumer group '{GroupName}'")]
    private partial void LogConsumerGroupJoined(string groupName);

    // Trace logs
    [LoggerMessage(
        EventId = 200,
        Level = LogLevel.Trace,
        Message = "Left consumer group '{GroupName}'")]
    private partial void LogLeftConsumerGroup(string groupName);

    // Warning logs
    [LoggerMessage(
        EventId = 302,
        Level = LogLevel.Warning,
        Message = "Failed to leave consumer group '{GroupName}'")]
    private partial void LogFailedToLeaveConsumerGroup(Exception exception, string groupName);

    [LoggerMessage(
        EventId = 303,
        Level = LogLevel.Warning,
        Message = "Failed to logout user or dispose client")]
    private partial void LogFailedToLogoutOrDispose(Exception exception);

    // Error logs
    [LoggerMessage(
        EventId = 400,
        Level = LogLevel.Error,
        Message = "Failed to initialize consumer group '{GroupName}'")]
    private partial void LogFailedToInitializeConsumerGroup(Exception exception, string groupName);

    [LoggerMessage(
        EventId = 401,
        Level = LogLevel.Error,
        Message = "Failed to create consumer group '{GroupName}'")]
    private partial void LogFailedToCreateConsumerGroup(Exception exception, string groupName);
}
