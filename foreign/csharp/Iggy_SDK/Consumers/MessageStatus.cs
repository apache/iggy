namespace Apache.Iggy.Consumers;

/// <summary>
/// Represents the status of a received message
/// </summary>
public enum MessageStatus
{
    /// <summary>
    /// Message was successfully received and processed
    /// </summary>
    Success,

    /// <summary>
    /// Message decryption failed
    /// </summary>
    DecryptionFailed,

    /// <summary>
    /// Message deserialization failed
    /// </summary>
    DeserializationFailed
}
