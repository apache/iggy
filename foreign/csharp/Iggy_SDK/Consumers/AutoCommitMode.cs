namespace Apache.Iggy.Consumers;

/// <summary>
/// Auto commit modes
/// </summary>
public enum AutoCommitMode
{
    /// <summary>
    /// Set auto commit to true on polling messages
    /// </summary>
    Auto,
    
    /// <summary>
    /// Set offset after polling messages
    /// </summary>
    AfterPoll,
    
    /// <summary>
    /// Set offset after receive message
    /// </summary>
    AfterReceive,
    
    /// <summary>
    /// Offset will not be store automaticly
    /// </summary>
    Disabled
}
