using System.Threading.Channels;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;

namespace Apache.Iggy.Publishers;

/// <summary>
///     Internal background processor that handles asynchronous message batching and sending.
///     Reads messages from a bounded channel and sends them in batches with retry support.
/// </summary>
internal sealed partial class BackgroundMessageProcessor : IAsyncDisposable
{
    // Logging methods
    [LoggerMessage(EventId = 10,
        Level = LogLevel.Debug,
        Message = "Background message processor started")]
    private partial void LogBackgroundProcessorStarted();

    [LoggerMessage(EventId = 11,
        Level = LogLevel.Debug,
        Message = "Background message processor cancelled")]
    private partial void LogBackgroundProcessorCancelled();

    [LoggerMessage(EventId = 12,
        Level = LogLevel.Debug,
        Message = "Background message processor stopped")]
    private partial void LogBackgroundProcessorStopped();

    [LoggerMessage(EventId = 15,
        Level = LogLevel.Debug,
        Message = "Waiting for background task to complete")]
    private partial void LogWaitingForBackgroundTask();

    [LoggerMessage(EventId = 16,
        Level = LogLevel.Debug,
        Message = "Background task completed")]
    private partial void LogBackgroundTaskCompleted();

    [LoggerMessage(EventId = 300,
        Level = LogLevel.Warning,
        Message = "Failed to send batch of {Count} messages (attempt {Attempt}/{MaxAttempts}). Retrying in {Delay}ms")]
    private partial void LogRetryingBatch(Exception exception, int count, int attempt, int maxAttempts, double delay);

    [LoggerMessage(EventId = 301,
        Level = LogLevel.Warning,
        Message = "Background task did not complete within timeout")]
    private partial void LogBackgroundTaskTimeout();

    [LoggerMessage(EventId = 403,
        Level = LogLevel.Error,
        Message = "Failed to send batch of {Count} messages")]
    private partial void LogFailedToSendBatch(Exception exception, int count);

    [LoggerMessage(EventId = 404,
        Level = LogLevel.Error,
        Message = "Unexpected error in background message processor")]
    private partial void LogBackgroundProcessorError(Exception exception);

    [LoggerMessage(EventId = 405,
        Level = LogLevel.Error,
        Message = "Failed to send batch of {Count} messages after {Attempts} attempts")]
    private partial void LogFailedToSendBatchAfterRetries(Exception exception, int count, int attempts);
}
