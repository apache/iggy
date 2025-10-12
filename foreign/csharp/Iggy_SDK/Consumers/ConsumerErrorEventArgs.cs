namespace Apache.Iggy.Consumers;

/// <summary>
///     Event arguments for consumer polling errors
/// </summary>
public class ConsumerErrorEventArgs : EventArgs
{
    /// <summary>
    ///     The exception that occurred during polling
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    ///     A descriptive message about the error
    /// </summary>
    public string Message { get; }

    /// <summary>
    ///     The UTC timestamp when the error occurred
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="ConsumerErrorEventArgs" /> class
    /// </summary>
    /// <param name="exception">The exception that occurred</param>
    /// <param name="message">A descriptive error message</param>
    public ConsumerErrorEventArgs(Exception exception, string message)
    {
        Exception = exception;
        Message = message;
        Timestamp = DateTime.UtcNow;
    }
}
