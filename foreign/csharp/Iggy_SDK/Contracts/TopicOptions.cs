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
using Apache.Iggy.IggyClient;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Options a topic takes at creation, passed to <see cref="IIggyTopic.CreateTopicAsync" />.
/// </summary>
/// <remarks>
///     <see cref="MessageExpiry" /> and <see cref="MaxTopicSize" /> can change later through a
///     <see cref="TopicUpdate" />, every other setting is fixed at creation. A property left null emits no key,
///     which leaves the server default in place. <see cref="IIggySystem.DescribeOptionsAsync" /> enumerates the
///     keys a given server accepts, with the kind, default and mutability of each.
/// </remarks>
public sealed class TopicOptions
{
    /// <summary>
    ///     Compression algorithm label stored with the topic. Fixed at creation.
    /// </summary>
    public CompressionAlgorithm? CompressionAlgorithm { get; init; }

    /// <summary>
    ///     How long a message is kept. <see cref="TimeSpan.MaxValue" /> never expires, zero keeps the server default.
    /// </summary>
    public TimeSpan? MessageExpiry { get; init; }

    /// <summary>
    ///     Topic-wide retained size cap in bytes, divided among partitions and at least the segment size.
    ///     <see cref="ulong.MaxValue" /> is unlimited, zero keeps the server default.
    /// </summary>
    public ulong? MaxTopicSize { get; init; }

    /// <summary>
    ///     Size of a single segment in bytes. The server checks it against its own bounds, a
    ///     multiple of 512 within the range it reports for the key.
    /// </summary>
    public ulong? SegmentSize { get; init; }

    /// <summary>
    ///     Message completion policy. Defaults to replicated independently of offset durability.
    /// </summary>
    public Durability Durability { get; init; } = Durability.Replicated;

    /// <summary>Explicit offset completion policy. Defaults to replicated independently of message durability.</summary>
    public Durability ConsumerOffsetDurability { get; init; } = Durability.Replicated;

    /// <summary>
    ///     Whether partitions can be added or deleted after creation. The server default is mutable.
    /// </summary>
    public PartitionResizePolicy? PartitionResizePolicy { get; init; }

    /// <summary>
    ///     Attempt a flush after this many messages accumulate in a partition buffer. Must be non-zero.
    /// </summary>
    public uint? MessagesRequiredToSave { get; init; }

    /// <summary>
    ///     Attempt a flush after this many bytes accumulate in a partition buffer. Paired with
    ///     <see cref="MessagesRequiredToSave" />; either threshold triggers an attempt.
    /// </summary>
    public ulong? SizeOfMessagesRequiredToSave { get; init; }

    /// <summary>
    ///     Request filesystem preallocation when creating a segment. The server may fall back
    ///     to extending the file when preallocation is unavailable.
    /// </summary>
    public bool? PreallocateSegments { get; init; }

    /// <summary>
    ///     Option keys with no property of their own, keyed by option name, for keys newer than this build. A
    ///     property above wins on collision, and the server refuses a key outside its catalog by name. A raw
    ///     durability is kept unless the matching property asks for persisted durability.
    /// </summary>
    [JsonConverter(typeof(RedactedOptionsConverter))]
    public IReadOnlyDictionary<string, HeaderValue>? Options { get; init; }

    /// <summary>
    ///     Renders the options that were set, each under the kind the server's catalog gives its key.
    /// </summary>
    /// <returns>Option values keyed by option name, including both durability policies.</returns>
    /// <exception cref="ArgumentException">A raw durability is invalid or contradicts a persisted property.</exception>
    public Dictionary<string, HeaderValue> ToDictionary()
    {
        var options = OptionKeys.Raw(Options);

        if (CompressionAlgorithm is { } compressionAlgorithm)
        {
            options[Name(TopicOptionKey.CompressionAlgorithm)] = HeaderValue.FromString(compressionAlgorithm switch
            {
                Enums.CompressionAlgorithm.None => "none",
                Enums.CompressionAlgorithm.Gzip => "gzip",
                _ => throw new ArgumentOutOfRangeException(nameof(CompressionAlgorithm), compressionAlgorithm,
                    "Unknown compression algorithm")
            });
        }

        if (MessageExpiry is { } messageExpiry && DurationHelpers.ToDuration(messageExpiry) is not 0 and var micros)
        {
            options[Name(TopicOptionKey.MessageExpiry)] = HeaderValue.FromUInt64(micros);
        }

        if (MaxTopicSize is { } maxTopicSize and not 0)
        {
            options[Name(TopicOptionKey.MaxTopicSize)] = HeaderValue.FromUInt64(maxTopicSize);
        }

        if (SegmentSize is { } segmentSize)
        {
            options[Name(TopicOptionKey.SegmentSize)] = HeaderValue.FromUInt64(segmentSize);
        }

        options[Name(TopicOptionKey.Durability)] = DurabilityValue(TopicOptionKey.Durability, Durability);
        options[Name(TopicOptionKey.ConsumerOffsetDurability)] =
            DurabilityValue(TopicOptionKey.ConsumerOffsetDurability, ConsumerOffsetDurability);

        if (PartitionResizePolicy is { } partitionResizePolicy)
        {
            options[Name(TopicOptionKey.PartitionResizePolicy)] = HeaderValue.FromString(partitionResizePolicy switch
            {
                Enums.PartitionResizePolicy.Mutable => "mutable",
                Enums.PartitionResizePolicy.Fixed => "fixed",
                _ => throw new ArgumentOutOfRangeException(nameof(PartitionResizePolicy), partitionResizePolicy,
                    "Unknown partition resize policy")
            });
        }

        if (MessagesRequiredToSave is { } messagesRequiredToSave)
        {
            options[Name(TopicOptionKey.MessagesRequiredToSave)] = HeaderValue.FromUInt32(messagesRequiredToSave);
        }

        if (SizeOfMessagesRequiredToSave is { } sizeOfMessagesRequiredToSave)
        {
            options[Name(TopicOptionKey.SizeOfMessagesRequiredToSave)] =
                HeaderValue.FromUInt64(sizeOfMessagesRequiredToSave);
        }

        if (PreallocateSegments is { } preallocateSegments)
        {
            options[Name(TopicOptionKey.PreallocateSegments)] = HeaderValue.FromBool(preallocateSegments);
        }

        return options;
    }

    private HeaderValue DurabilityValue(TopicOptionKey key, Durability policy)
    {
        var name = Name(key);
        if (Options is not null && Options.TryGetValue(name, out var raw))
        {
            var rawPolicy = raw.Kind == HeaderKind.String
                ? raw.ToString() switch
                {
                    "replicated" => Durability.Replicated,
                    "persisted" => Durability.Persisted,
                    _ => (Durability?)null
                }
                : null;
            if (rawPolicy is null || (policy == Durability.Persisted && rawPolicy != policy))
            {
                throw new ArgumentException($"Invalid {name}", nameof(Options));
            }

            policy = rawPolicy.Value;
        }

        return HeaderValue.FromString(policy == Durability.Persisted ? "persisted" : "replicated");
    }

    private static string Name(TopicOptionKey key)
    {
        return OptionKeys.Describe(key).Name;
    }
}
