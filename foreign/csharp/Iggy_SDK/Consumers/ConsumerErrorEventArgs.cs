namespace Apache.Iggy.Consumers;

public class ConsumerErrorEventArgs : EventArgs
{
    public Exception Exception { get; }
    public string Message { get; }
    public DateTime Timestamp { get; }

    public ConsumerErrorEventArgs(Exception exception, string message)
    {
        Exception = exception;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }
}