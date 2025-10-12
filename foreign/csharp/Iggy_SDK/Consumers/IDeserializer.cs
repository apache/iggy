namespace Apache.Iggy.Consumers;

/// <summary>
///     Interface for deserializing message payloads from byte arrays to type T.
///     <para>
///         No type constraints are enforced on T to provide maximum flexibility.
///         Implementations are responsible for ensuring that the provided byte data can be properly deserialized to the target type.
///     </para>
/// </summary>
/// <typeparam name="T">
///     The target type for deserialization. Can be any type - reference or value type, nullable or non-nullable.
///     The deserializer implementation must be able to produce instances of the specific type.
/// </typeparam>
/// <remarks>
///     Implementations should throw appropriate exceptions (e.g., <see cref="System.FormatException" />,
///     <see cref="System.ArgumentException" />, or <see cref="System.InvalidOperationException" />)
///     if the provided data cannot be deserialized to type T. These exceptions will be caught and logged by
///     <see cref="IggyConsumer{T}" /> during message processing.
/// </remarks>
public interface IDeserializer<out T>
{
    /// <summary>
    ///     Deserializes a byte array into an instance of type T.
    /// </summary>
    /// <param name="data">The byte array containing the serialized data to deserialize.</param>
    /// <returns>An instance of type T representing the deserialized data.</returns>
    /// <exception cref="System.FormatException">
    ///     Thrown when the data format is invalid and cannot be deserialized.
    /// </exception>
    /// <exception cref="System.ArgumentException">
    ///     Thrown when the data cannot be deserialized due to invalid content or structure.
    /// </exception>
    /// <exception cref="System.InvalidOperationException">
    ///     Thrown when the deserialization operation fails due to state issues.
    /// </exception>
    T Deserialize(byte[] data);
}
