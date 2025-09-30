namespace Apache.Iggy.Consumers;

public class MessageDecryptionFailedEventArgs : EventArgs
{
    public Exception Exception { get; }
    public ReceivedMessage FailedMessage { get; }
    public DateTime Timestamp { get; }

    public MessageDecryptionFailedEventArgs(Exception exception, ReceivedMessage failedMessage)
    {
        Exception = exception;
        FailedMessage = failedMessage;
        Timestamp = DateTime.UtcNow;
    }
}