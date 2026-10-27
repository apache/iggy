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
///     Helpers for the per-stream encryption key passed in <see cref="Contracts.StreamOptions.EncryptionKey" />.
/// </summary>
/// <remarks>
///     A stream key is 32 random bytes. It travels inside the create request over whatever transport the client
///     uses, so protecting it on the wire with TLS is the caller's choice. The server stores it wrapped and never
///     returns it: reads report only its source and fingerprints. Never derive a key from a password, the
///     fingerprint would then let anyone who can read the stream test guesses offline.
/// </remarks>
public static class EncryptionKeys
{
    /// <summary>Length of a stream encryption key in bytes.</summary>
    public const int KEY_LENGTH = 32;

    private static readonly byte[] FingerprintLabel = "iggy/stream-key-fingerprint/v1"u8.ToArray();

    /// <summary>
    ///     The fingerprint a stream reports in <see cref="Contracts.StreamResponse.EncryptionKeyFingerprint" /> for
    ///     this key: the first 8 bytes of SHA-256 over a fixed label and the key, as 16 lowercase hex characters.
    /// </summary>
    /// <param name="key">The 32 key bytes.</param>
    /// <exception cref="ArgumentException">The key is not exactly 32 bytes.</exception>
    public static string Fingerprint(ReadOnlySpan<byte> key)
    {
        if (key.Length != KEY_LENGTH)
        {
            throw new ArgumentException($"A stream encryption key must be exactly {KEY_LENGTH} bytes.", nameof(key));
        }

        var input = new byte[FingerprintLabel.Length + KEY_LENGTH];
        try
        {
            FingerprintLabel.CopyTo(input, 0);
            key.CopyTo(input.AsSpan(FingerprintLabel.Length));
            return Convert.ToHexString(SHA256.HashData(input), 0, 8).ToLowerInvariant();
        }
        finally
        {
            CryptographicOperations.ZeroMemory(input);
        }
    }

    /// <summary>The fingerprint of a key given in canonical standard base64.</summary>
    /// <param name="base64Key">The key as padded standard base64 of exactly 32 bytes.</param>
    /// <exception cref="ArgumentException">The text is not the canonical base64 of 32 bytes.</exception>
    public static string Fingerprint(string base64Key)
    {
        var key = FromBase64(base64Key);
        try
        {
            return Fingerprint(key);
        }
        finally
        {
            CryptographicOperations.ZeroMemory(key);
        }
    }

    /// <summary>
    ///     Decodes a key from strict canonical standard base64: padded, no whitespace, no URL-safe alphabet and
    ///     no stray bits in the last character.
    /// </summary>
    /// <param name="base64Key">The key as padded standard base64 of exactly 32 bytes.</param>
    /// <returns>The 32 key bytes.</returns>
    /// <exception cref="ArgumentException">The text is not the canonical base64 of 32 bytes.</exception>
    public static byte[] FromBase64(string base64Key)
    {
        // Room for one extra decoded byte, so a longer key fails the length check instead of the decode.
        var buffer = new byte[KEY_LENGTH + 1];
        try
        {
            if (!Convert.TryFromBase64String(base64Key, buffer, out var written) || written != KEY_LENGTH
                || Convert.ToBase64String(buffer, 0, KEY_LENGTH) != base64Key)
            {
                throw new ArgumentException(
                    $"A stream encryption key must be the canonical padded base64 of exactly {KEY_LENGTH} bytes.",
                    nameof(base64Key));
            }

            return buffer[..KEY_LENGTH];
        }
        finally
        {
            CryptographicOperations.ZeroMemory(buffer);
        }
    }
}
