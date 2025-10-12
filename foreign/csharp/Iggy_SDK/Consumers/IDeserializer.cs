namespace Apache.Iggy.Consumers;

/// <summary>
///     Interface for deserializing message payloads from byte arrays to type T
/// </summary>
/// <typeparam name="T">The target type for deserialization</typeparam>
public interface IDeserializer<out T>
{
    /// <summary>
    ///     Deserializes a byte array into an instance of type T
    /// </summary>
    /// <param name="data">The byte array to deserialize</param>
    /// <returns>An instance of type T</returns>
    T Deserialize(byte[] data);
}
