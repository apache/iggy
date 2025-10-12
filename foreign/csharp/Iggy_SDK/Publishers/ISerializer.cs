namespace Apache.Iggy.Publishers;

/// <summary>
///     Interface for serializing objects of type T to byte arrays
/// </summary>
/// <typeparam name="T">The source type for serialization</typeparam>
public interface ISerializer<in T>
{
    /// <summary>
    ///     Serializes an instance of type T into a byte array
    /// </summary>
    /// <param name="data">The object to serialize</param>
    /// <returns>A byte array representing the serialized data</returns>
    byte[] Serialize(T data);
}
