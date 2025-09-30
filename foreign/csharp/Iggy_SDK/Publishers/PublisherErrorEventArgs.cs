namespace Apache.Iggy.Publishers;

public class PublisherErrorEventArgs : EventArgs
{
    public Exception Exception { get; }
    public string Message { get; }
    public DateTime Timestamp { get; }

    public PublisherErrorEventArgs(Exception exception, string message)
    {
        Exception = exception;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }
}