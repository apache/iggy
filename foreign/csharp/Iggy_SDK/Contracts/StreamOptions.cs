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

using System.Text.Json.Serialization;
using Apache.Iggy.Encryption;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Options a stream takes at creation, passed to <see cref="IIggyStream.CreateStreamAsync" />.
/// </summary>
public sealed class StreamOptions
{
    /// <summary>
    ///     The stream's own data encryption key: exactly 32 random bytes, see <see cref="EncryptionKeys" />.
    /// </summary>
    /// <remarks>
    ///     The key travels inside the create request over the transport the client uses, so TLS is the caller's
    ///     responsibility. The server stores it wrapped and never returns it, reads report only
    ///     <see cref="StreamResponse.EncryptionKeySource" /> and its fingerprints. Left null, a server with
    ///     encryption enabled generates a key for the stream, otherwise the stream is stored in plaintext.
    ///     The key cannot be changed after creation, and JSON serialization of these options leaves it out and
    ///     writes <see cref="Options" /> entries by name only, so a structured logger never writes it.
    /// </remarks>
    [JsonIgnore]
    public byte[]? EncryptionKey { get; init; }

    /// <summary>
    ///     Option keys with no property of their own, keyed by option name, for keys newer than this build. A
    ///     property above wins on collision, and the server refuses a key outside its catalog by name.
    /// </summary>
    [JsonConverter(typeof(RedactedOptionsConverter))]
    public IReadOnlyDictionary<string, HeaderValue>? Options { get; init; }

    /// <summary>
    ///     Renders the options that were set, each under the kind the server's catalog gives its key.
    /// </summary>
    /// <returns>Option values keyed by option name.</returns>
    /// <exception cref="Exceptions.IggyInvalidStatusCodeException">The encryption key is not exactly 32 bytes.</exception>
    public Dictionary<string, HeaderValue> ToDictionary()
    {
        var options = OptionKeys.Raw(Options);
        if (EncryptionKey is { } key)
        {
            if (key.Length != EncryptionKeys.KEY_LENGTH)
            {
                throw VsrError.Exception(VsrError.INVALID_ENCRYPTION_KEY,
                    $"A stream encryption key must be exactly {EncryptionKeys.KEY_LENGTH} bytes.");
            }

            options[OptionKeys.Describe(StreamOptionKey.EncryptionKey).Name] = HeaderValue.FromBytes([.. key]);
        }

        return options;
    }
}
