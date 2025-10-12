namespace Apache.Iggy.Publishers;

/// <summary>
///     Event arguments for publisher error events.
/// </summary>
public class PublisherErrorEventArgs : EventArgs
{
    /// <summary>
    ///     Gets the exception that occurred.
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    ///     Gets a descriptive message about the error.
    /// </summary>
    public string Message { get; }

    /// <summary>
    ///     Gets the UTC timestamp when the error occurred.
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="PublisherErrorEventArgs" /> class.
    /// </summary>
    /// <param name="exception">The exception that occurred.</param>
    /// <param name="message">A descriptive message about the error.</param>
    public PublisherErrorEventArgs(Exception exception, string message)
    {
        Exception = exception;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }
}
