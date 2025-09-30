using Apache.Iggy.Messages;

namespace Apache.Iggy.Publishers;

public class MessageBatchFailedEventArgs : EventArgs
{
    public Exception Exception { get; }
    public Message[] FailedMessages { get; }
    public DateTime Timestamp { get; }

    public MessageBatchFailedEventArgs(Exception exception, Message[] failedMessages)
    {
        Exception = exception;
        FailedMessages = failedMessages;
        Timestamp = DateTime.UtcNow;
    }
}