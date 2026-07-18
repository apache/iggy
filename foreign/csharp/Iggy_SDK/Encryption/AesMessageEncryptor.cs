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
///     AES-GCM based message encryptor for secure message encryption/decryption.
///     Uses AES-GCM (Galois/Counter Mode) which provides both confidentiality and authenticity.
///     Output format: [12-byte nonce][ciphertext][16-byte authentication tag].
/// </summary>
/// <remarks>
///     Nonces are random per invocation, so NIST SP 800-38D caps a single key at 2^32 encryptions before
///     nonce-collision probability grows unacceptable; a message with headers spends two invocations. Rotate
///     the key well before that bound: a collision breaks confidentiality and integrity for the whole key.
///     Dispose to release the per-thread cipher contexts and zero the key copy.
/// </remarks>
public sealed class AesMessageEncryptor : IMessageEncryptor, IDisposable
{
    private const int NonceSize = 12; // 96 bits - recommended for GCM
    private const int TagSize = 16; // 128 bits authentication tag

    // AesGcm instances are not thread-safe. One per thread lets a shared encryptor serve concurrent threads while
    // reusing the native key schedule instead of rebuilding a cipher per call. trackAllValues so Dispose can
    // release every thread's context.
    private readonly ThreadLocal<AesGcm> _aesGcm;
    private readonly byte[] _key;
    private int _disposed;

    /// <summary>
    ///     Creates a new AES message encryptor with the specified key.
    /// </summary>
    /// <param name="key">The encryption key. Must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256 respectively.</param>
    /// <exception cref="ArgumentNullException">Thrown when key is null</exception>
    /// <exception cref="ArgumentException">Thrown when key length is invalid</exception>
    public AesMessageEncryptor(byte[] key)
    {
        ArgumentNullException.ThrowIfNull(key);

        if (key.Length != 16 && key.Length != 24 && key.Length != 32)
        {
            throw new ArgumentException("Key must be 16, 24, or 32 bytes for AES-128, AES-192, or AES-256",
                nameof(key));
        }

        // Own copy: caller mutating its array must not desync lazily-created ciphers, and Dispose can zero this
        // without clobbering caller memory.
        _key = (byte[])key.Clone();
        _aesGcm = new ThreadLocal<AesGcm>(() => new AesGcm(_key, TagSize), true);
    }

    /// <summary>
    ///     Releases every thread's native cipher context and zeroes the internal key copy. The encryptor must not
    ///     be used concurrently with or after disposal.
    /// </summary>
    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        foreach (var aesGcm in _aesGcm.Values)
        {
            aesGcm.Dispose();
        }

        _aesGcm.Dispose();
        CryptographicOperations.ZeroMemory(_key);
        GC.SuppressFinalize(this);
    }

    /// <summary>
    ///     Exact ciphertext length for AES-GCM: plaintext plus nonce and tag.
    /// </summary>
    public int GetMaxEncryptedLength(int plaintextLength)
    {
        return NonceSize + plaintextLength + TagSize;
    }

    /// <summary>
    ///     Exact plaintext length for AES-GCM: ciphertext minus nonce and tag.
    /// </summary>
    public int GetMaxDecryptedLength(int encryptedLength)
    {
        return Math.Max(0, encryptedLength - NonceSize - TagSize);
    }

    /// <summary>
    ///     Encrypts <paramref name="plaintext" /> with AES-GCM into <paramref name="destination" />, returning the
    ///     number of bytes written. Format: [12-byte nonce][encrypted data][16-byte authentication tag].
    /// </summary>
    public int Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> destination)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

        var totalLength = NonceSize + plaintext.Length + TagSize;
        Span<byte> nonce = destination[..NonceSize];
        Span<byte> ciphertext = destination.Slice(NonceSize, plaintext.Length);
        Span<byte> tag = destination.Slice(NonceSize + plaintext.Length, TagSize);

        RandomNumberGenerator.Fill(nonce);

        _aesGcm.Value!.Encrypt(nonce, plaintext, ciphertext, tag);

        return totalLength;
    }

    /// <summary>
    ///     Decrypts <paramref name="encryptedData" /> into <paramref name="destination" /> using AES-GCM, returning
    ///     the number of plaintext bytes written.
    /// </summary>
    /// <exception cref="ArgumentException">Thrown when encrypted data format is invalid</exception>
    /// <exception cref="CryptographicException">Thrown when decryption or authentication fails</exception>
    public int Decrypt(ReadOnlySpan<byte> encryptedData, Span<byte> destination)
    {
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) == 1, this);

        if (encryptedData.Length < NonceSize + TagSize)
        {
            throw new ArgumentException("Encrypted data is too short to contain nonce and tag", nameof(encryptedData));
        }

        var plaintextLength = encryptedData.Length - NonceSize - TagSize;
        ReadOnlySpan<byte> nonce = encryptedData[..NonceSize];
        ReadOnlySpan<byte> ciphertext = encryptedData.Slice(NonceSize, plaintextLength);
        ReadOnlySpan<byte> tag = encryptedData.Slice(NonceSize + plaintextLength, TagSize);

        _aesGcm.Value!.Decrypt(nonce, ciphertext, tag, destination[..plaintextLength]);

        return plaintextLength;
    }

    /// <summary>
    ///     Creates a new AES message encryptor with the specified base64-encoded key.
    /// </summary>
    /// <param name="base64Key">The base64-encoded encryption key; must decode to 16, 24, or 32 bytes</param>
    /// <returns>A new AesMessageEncryptor instance</returns>
    /// <exception cref="FormatException">Thrown when the key is not valid base64</exception>
    /// <exception cref="ArgumentException">Thrown when the decoded key length is invalid</exception>
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
