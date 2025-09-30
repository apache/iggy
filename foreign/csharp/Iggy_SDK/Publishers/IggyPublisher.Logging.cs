using Apache.Iggy.Enums;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

public partial class IggyPublisher
{
    // Debug logs
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Debug,
        Message = "Initializing background message sending with queue capacity: {Capacity}, batch size: {BatchSize}")]
    private partial void LogInitializingBackgroundSending(int capacity, int batchSize);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Debug,
        Message = "Publisher already initialized")]
    private partial void LogPublisherAlreadyInitialized();

    [LoggerMessage(
        EventId = 3,
        Level = LogLevel.Debug,
        Message = "User {Login} logged in successfully")]
    private partial void LogUserLoggedIn(string login);

    [LoggerMessage(
        EventId = 4,
        Level = LogLevel.Debug,
        Message = "Stream {StreamId} already exists")]
    private partial void LogStreamAlreadyExists(Identifier streamId);

    [LoggerMessage(
        EventId = 5,
        Level = LogLevel.Debug,
        Message = "Topic {TopicId} already exists in stream {StreamId}")]
    private partial void LogTopicAlreadyExists(Identifier topicId, Identifier streamId);

    [LoggerMessage(
        EventId = 7,
        Level = LogLevel.Debug,
        Message = "Successfully sent {Count} messages")]
    private partial void LogSuccessfullySentMessages(int count);

    [LoggerMessage(
        EventId = 8,
        Level = LogLevel.Debug,
        Message = "Waiting for all pending messages to be sent")]
    private partial void LogWaitingForPendingMessages();

    [LoggerMessage(
        EventId = 9,
        Level = LogLevel.Debug,
        Message = "All pending messages have been sent")]
    private partial void LogAllPendingMessagesSent();

    [LoggerMessage(
        EventId = 10,
        Level = LogLevel.Debug,
        Message = "Background message processor started")]
    private partial void LogBackgroundProcessorStarted();

    [LoggerMessage(
        EventId = 11,
        Level = LogLevel.Debug,
        Message = "Background message processor cancelled")]
    private partial void LogBackgroundProcessorCancelled();

    [LoggerMessage(
        EventId = 12,
        Level = LogLevel.Debug,
        Message = "Background message processor stopped")]
    private partial void LogBackgroundProcessorStopped();

    [LoggerMessage(
        EventId = 14,
        Level = LogLevel.Debug,
        Message = "Disposing publisher")]
    private partial void LogDisposingPublisher();

    [LoggerMessage(
        EventId = 15,
        Level = LogLevel.Debug,
        Message = "Waiting for background task to complete")]
    private partial void LogWaitingForBackgroundTask();

    [LoggerMessage(
        EventId = 16,
        Level = LogLevel.Debug,
        Message = "Background task completed")]
    private partial void LogBackgroundTaskCompleted();

    // Information logs
    [LoggerMessage(
        EventId = 100,
        Level = LogLevel.Information,
        Message = "Initializing publisher for stream: {StreamId}, topic: {TopicId}")]
    private partial void LogInitializingPublisher(Identifier streamId, Identifier topicId);

    [LoggerMessage(
        EventId = 101,
        Level = LogLevel.Information,
        Message = "Background message sending started")]
    private partial void LogBackgroundSendingStarted();

    [LoggerMessage(
        EventId = 102,
        Level = LogLevel.Information,
        Message = "Publisher initialized successfully")]
    private partial void LogPublisherInitialized();

    [LoggerMessage(
        EventId = 103,
        Level = LogLevel.Information,
        Message = "Creating stream {StreamId} with name: {StreamName}")]
    private partial void LogCreatingStream(Identifier streamId, string streamName);

    [LoggerMessage(
        EventId = 104,
        Level = LogLevel.Information,
        Message = "Stream {StreamId} created successfully")]
    private partial void LogStreamCreated(Identifier streamId);

    [LoggerMessage(
        EventId = 105,
        Level = LogLevel.Information,
        Message = "Creating topic {TopicId} with name: {TopicName} in stream {StreamId}")]
    private partial void LogCreatingTopic(Identifier topicId, string topicName, Identifier streamId);

    [LoggerMessage(
        EventId = 106,
        Level = LogLevel.Information,
        Message = "Topic {TopicId} created successfully in stream {StreamId}")]
    private partial void LogTopicCreated(Identifier topicId, Identifier streamId);
    

    [LoggerMessage(
        EventId = 108,
        Level = LogLevel.Information,
        Message = "Publisher disposed")]
    private partial void LogPublisherDisposed();

    // Trace logs
    [LoggerMessage(
        EventId = 200,
        Level = LogLevel.Trace,
        Message = "Queuing {Count} messages for background sending")]
    private partial void LogQueuingMessages(int count);
    

    // Warning logs
    [LoggerMessage(
        EventId = 300,
        Level = LogLevel.Warning,
        Message = "Failed to send batch of {Count} messages (attempt {Attempt}/{MaxAttempts}). Retrying in {Delay}ms")]
    private partial void LogRetryingBatch(Exception exception, int count, int attempt, int maxAttempts, double delay);

    [LoggerMessage(
        EventId = 301,
        Level = LogLevel.Warning,
        Message = "Background task did not complete within timeout")]
    private partial void LogBackgroundTaskTimeout();

    // Error logs
    [LoggerMessage(
        EventId = 400,
        Level = LogLevel.Error,
        Message = "Stream {StreamId} does not exist and auto-creation is disabled")]
    private partial void LogStreamDoesNotExist(Identifier streamId);

    [LoggerMessage(
        EventId = 401,
        Level = LogLevel.Error,
        Message = "Topic {TopicId} does not exist in stream {StreamId} and auto-creation is disabled")]
    private partial void LogTopicDoesNotExist(Identifier topicId, Identifier streamId);

    [LoggerMessage(
        EventId = 402,
        Level = LogLevel.Error,
        Message = "Attempted to send messages before publisher initialization")]
    private partial void LogSendBeforeInitialization();

    [LoggerMessage(
        EventId = 403,
        Level = LogLevel.Error,
        Message = "Failed to send batch of {Count} messages")]
    private partial void LogFailedToSendBatch(Exception exception, int count);

    [LoggerMessage(
        EventId = 404,
        Level = LogLevel.Error,
        Message = "Unexpected error in background message processor")]
    private partial void LogBackgroundProcessorError(Exception exception);

    [LoggerMessage(
        EventId = 405,
        Level = LogLevel.Error,
        Message = "Failed to send batch of {Count} messages after {Attempts} attempts")]
    private partial void LogFailedToSendBatchAfterRetries(Exception exception, int count, int attempts);
}
