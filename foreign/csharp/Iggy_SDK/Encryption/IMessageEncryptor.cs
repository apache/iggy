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

namespace Apache.Iggy.Encryption;

/// <summary>
///     Encrypts and decrypts message payloads (and user headers) at the wire boundary: the client encrypts
///     directly into the outgoing wire buffer on send and decrypts into a pooled buffer on poll, so neither
///     direction allocates per message. Implementations write into a caller-provided destination sized by the
///     corresponding GetMax*Length bound and return the number of bytes actually written.
/// </summary>
/// <remarks>
///     Implementations must be thread-safe: a single instance is shared by every publisher and consumer on the
///     client and may encrypt or decrypt on concurrent threads. Nothing enforces this at the boundary: an
///     implementation that shares mutable cipher state across calls will silently corrupt ciphertext or crash
///     under concurrent send/poll. See <see cref="AesMessageEncryptor" /> for the built-in AEAD implementation.
/// </remarks>
public interface IMessageEncryptor
{
    /// <summary>
    ///     A safe upper bound on the ciphertext length produced by encrypting <paramref name="plaintextLength" />
    ///     bytes, used to size the wire buffer before encryption runs. Exact for AEAD ciphers
    ///     (plaintext + nonce + tag).
    /// </summary>
    int GetMaxEncryptedLength(int plaintextLength);

    /// <summary>
    ///     A safe upper bound on the plaintext length produced by decrypting <paramref name="encryptedLength" />
    ///     bytes, used to size the pooled buffer the poll path decrypts into.
    /// </summary>
    int GetMaxDecryptedLength(int encryptedLength);

    /// <summary>
    ///     Encrypts <paramref name="plaintext" /> into <paramref name="destination" /> and returns the number of
    ///     bytes written. The destination is at least <see cref="GetMaxEncryptedLength" /> bytes and does not
    ///     overlap the source.
    /// </summary>
    int Encrypt(ReadOnlySpan<byte> plaintext, Span<byte> destination);

    /// <summary>
    ///     Decrypts <paramref name="encryptedData" /> into <paramref name="destination" /> and returns the number
    ///     of bytes written. The destination is at least <see cref="GetMaxDecryptedLength" /> bytes and does not
    ///     overlap the source. Throws on authentication failure or malformed input.
    /// </summary>
    int Decrypt(ReadOnlySpan<byte> encryptedData, Span<byte> destination);
}
