// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

using System.Security.Cryptography;

namespace Apache.Iggy.Encryption;

/// <summary>
///     AES-256-GCM based message encryptor for secure message encryption/decryption.
///     Uses AES-GCM (Galois/Counter Mode) which provides both confidentiality and authenticity.
/// </summary>
public sealed class AesMessageEncryptor : IMessageEncryptor
{
    private const int NonceSize = 12; // 96 bits - recommended for GCM
    private const int TagSize = 16; // 128 bits authentication tag
    private readonly byte[] _key;

    /// <summary>
    ///     Creates a new AES message encryptor with the specified key.
    /// </summary>
    /// <param name="key">The encryption key. Must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256 respectively.</param>
    /// <exception cref="ArgumentException">Thrown when key length is invalid</exception>
    public AesMessageEncryptor(byte[] key)
    {
        if (key.Length != 16 && key.Length != 24 && key.Length != 32)
        {
            throw new ArgumentException("Key must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256",
                nameof(key));
        }

        _key = key;
    }

    /// <summary>
    ///     Encrypts the provided plain data using AES-GCM.
    ///     Format: [12-byte nonce][encrypted data][16-byte authentication tag]
    /// </summary>
    /// <param name="plainData">The plain data to encrypt</param>
    /// <returns>The encrypted data with nonce and tag</returns>
    public byte[] Encrypt(Span<byte> plainData)
    {
        using var aesGcm = new AesGcm(_key, TagSize);

        var nonce = new byte[NonceSize];
        RandomNumberGenerator.Fill(nonce);

        var ciphertext = new byte[plainData.Length];
        var tag = new byte[TagSize];

        aesGcm.Encrypt(nonce, plainData, ciphertext, tag);

        // Combine: nonce + ciphertext + tag
        var result = new byte[NonceSize + ciphertext.Length + TagSize];
        Buffer.BlockCopy(nonce, 0, result, 0, NonceSize);
        Buffer.BlockCopy(ciphertext, 0, result, NonceSize, ciphertext.Length);
        Buffer.BlockCopy(tag, 0, result, NonceSize + ciphertext.Length, TagSize);

        return result;
    }

    /// <summary>
    ///     Decrypts the provided encrypted data using AES-GCM.
    ///     Expected format: [12-byte nonce][encrypted data][16-byte authentication tag]
    /// </summary>
    /// <param name="encryptedData">The encrypted data with nonce and tag</param>
    /// <returns>The decrypted plain data</returns>
    /// <exception cref="ArgumentException">Thrown when encrypted data format is invalid</exception>
    /// <exception cref="CryptographicException">Thrown when decryption or authentication fails</exception>
    public byte[] Decrypt(ReadOnlySpan<byte> encryptedData)
    {
        if (encryptedData.Length < NonceSize + TagSize)
        {
            throw new ArgumentException("Encrypted data is too short to contain nonce and tag", nameof(encryptedData));
        }

        using var aesGcm = new AesGcm(_key, TagSize);

        // Extract nonce, ciphertext, and tag
        Span<byte> nonce = stackalloc byte[NonceSize];
        Span<byte> tag = stackalloc byte[TagSize];
        var ciphertextLength = encryptedData.Length - NonceSize - TagSize;
        Span<byte> ciphertext = stackalloc byte[ciphertextLength];

        encryptedData[..NonceSize].CopyTo(nonce);
        encryptedData.Slice(NonceSize, ciphertextLength).CopyTo(ciphertext);
        encryptedData.Slice(NonceSize + ciphertextLength, TagSize).CopyTo(tag);

        var plaintext = new byte[ciphertextLength];
        aesGcm.Decrypt(nonce, ciphertext, tag, plaintext);

        return plaintext;
    }

    /// <summary>
    ///     Creates a new AES-256 message encryptor with the specified base64-encoded key.
    /// </summary>
    /// <param name="base64Key">The base64-encoded encryption key</param>
    /// <returns>A new AesMessageEncryptor instance</returns>
    public static AesMessageEncryptor FromBase64Key(string base64Key)
    {
        return new AesMessageEncryptor(Convert.FromBase64String(base64Key));
    }

    /// <summary>
    ///     Generates a new random AES-256 key.
    /// </summary>
    /// <returns>A 32-byte random key suitable for AES-256</returns>
    public static byte[] GenerateKey()
    {
        var key = new byte[32];
        using var rng = RandomNumberGenerator.Create();
        rng.GetBytes(key);
        return key;
    }
}
