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
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;
using Apache.Iggy.JsonConverters;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Information about a stream.
/// </summary>
public sealed class StreamResponse
{
    /// <summary>
    ///     Stream identifier.
    /// </summary>
    public required uint Id { get; init; }

    /// <summary>
    ///     Unique stream name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    ///     Stream size in bytes.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong Size { get; init; }

    /// <summary>
    ///     Stream creation date.
    /// </summary>
    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    ///     Number of messages in the stream.
    /// </summary>
    public required ulong MessagesCount { get; init; }

    /// <summary>
    ///     Number of topics in the stream.
    /// </summary>
    public required uint TopicsCount { get; init; }

    /// <summary>
    ///     List of topics in the stream.
    /// </summary>
    public IEnumerable<TopicResponse> Topics { get; init; } = [];

    /// <summary>
    ///     Options the client set on the stream. The server never returns the encryption key.
    /// </summary>
    [JsonIgnore]
    public Dictionary<HeaderKey, HeaderValue>? Options { get; init; }

    /// <summary>
    ///     Options the server filled or computes on reads, such as the encryption key source and fingerprints.
    /// </summary>
    [JsonIgnore]
    public Dictionary<HeaderKey, HeaderValue>? DerivedOptions { get; init; }

    /// <summary>
    ///     Who chose the key that encrypts the stream at rest, or null when the server reported no source.
    /// </summary>
    [JsonIgnore]
    public StreamEncryptionSource? EncryptionKeySource =>
        OptionValues.EncryptionKeySource(Options, DerivedOptions);

    /// <summary>
    ///     Fingerprint of the stream's key, the value <see cref="Encryption.EncryptionKeys.Fingerprint(ReadOnlySpan{byte})" />
    ///     computes from it. Null for a plaintext stream.
    /// </summary>
    [JsonIgnore]
    public string? EncryptionKeyFingerprint =>
        OptionValues.Text(Options, DerivedOptions, StreamOptionKey.EncryptionKeyFingerprint);

    /// <summary>
    ///     Fingerprints of the server keys that wrap the stream's key: one outside a key rotation, empty for a
    ///     plaintext stream.
    /// </summary>
    [JsonIgnore]
    public IReadOnlyList<string> EncryptionKekFingerprints =>
        OptionValues.Text(Options, DerivedOptions, StreamOptionKey.EncryptionKekFingerprint) is { Length: > 0 } joined
            ? joined.Split(',')
            : [];

    /// <summary>
    ///     REST renders both provenances into one object keyed by option name, so one JSON property
    ///     fills <see cref="Options" /> and <see cref="DerivedOptions" />.
    /// </summary>
    [JsonInclude]
    [JsonPropertyName("options")]
    [JsonConverter(typeof(ResourceOptionsConverter))]
    internal ResourceOptions RestOptions
    {
        init
        {
            Options = value.Explicit;
            DerivedOptions = value.Derived;
        }
    }
}
