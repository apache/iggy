namespace Apache.Iggy.Consumers;

/// <summary>
///     Event arguments for message decryption failures
/// </summary>
public class MessageDecryptionFailedEventArgs : EventArgs
{
    /// <summary>
    ///     The exception that occurred during decryption
    /// </summary>
    public Exception Exception { get; }

    /// <summary>
    ///     The message that failed to decrypt
    /// </summary>
    public ReceivedMessage FailedMessage { get; }

    /// <summary>
    ///     The UTC timestamp when the decryption failure occurred
    /// </summary>
    public DateTime Timestamp { get; }

    /// <summary>
    ///     Initializes a new instance of the <see cref="MessageDecryptionFailedEventArgs" /> class
    /// </summary>
    /// <param name="exception">The exception that occurred during decryption</param>
    /// <param name="failedMessage">The message that failed to decrypt</param>
    public MessageDecryptionFailedEventArgs(Exception exception, ReceivedMessage failedMessage)
    {
        Exception = exception;
        FailedMessage = failedMessage;
        Timestamp = DateTime.UtcNow;
    }
}
