namespace Apache.Iggy.Encryption;

/// <summary>
/// Interface for encrypting and decrypting message payloads in Iggy messaging system.
/// Implementations of this interface can be used with IggyPublisher and IggyConsumer
/// to provide end-to-end encryption of message data.
/// </summary>
public interface IMessageEncryptor
{
    /// <summary>
    /// Encrypts the provided plain data.
    /// </summary>
    /// <param name="plainData">The plain data to encrypt</param>
    /// <returns>The encrypted data</returns>
    byte[] Encrypt(byte[] plainData);

    /// <summary>
    /// Decrypts the provided encrypted data.
    /// </summary>
    /// <param name="encryptedData">The encrypted data to decrypt</param>
    /// <returns>The decrypted plain data</returns>
    byte[] Decrypt(byte[] encryptedData);
}