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

using Apache.Iggy.Headers;
using Apache.Iggy.Enums;
using Apache.Iggy.IggyClient;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Topic options that have no named parameter of their own, typed instead of keyed by hand.
/// </summary>
/// <remarks>
///     Hand the result of <see cref="ToDictionary" /> to the <c>options</c> parameter of
///     <see cref="IIggyTopic.CreateTopicAsync" />. Every key here is settable at creation only: an
///     update takes just the options that have a parameter of their own and refuses the rest by
///     name, the same way it refuses a key outside the server's catalog, so a mistyped key fails
///     the call rather than being ignored. <see cref="IIggySystem.DescribeOptionsAsync" />
///     enumerates the keys a given server accepts, with the kind and default of each.
///     A property left null emits no key, which leaves the server default in place.
/// </remarks>
public sealed class TopicOptions
{
    private const string SegmentSizeKey = "segment_size";
    private const string DurabilityKey = "durability";
    private const string ConsumerOffsetDurabilityKey = "consumer_offset_durability";
    private const string MessagesRequiredToSaveKey = "messages_required_to_save";
    private const string SizeOfMessagesRequiredToSaveKey = "size_of_messages_required_to_save";
    private const string PreallocateSegmentsKey = "preallocate_segments";

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
    ///     Flush the journal once it holds this many messages. Must be non-zero.
    /// </summary>
    public uint? MessagesRequiredToSave { get; init; }

    /// <summary>
    ///     Flush the journal once it holds this many bytes. Paired with
    ///     <see cref="MessagesRequiredToSave" />: whichever threshold trips first flushes.
    /// </summary>
    public ulong? SizeOfMessagesRequiredToSave { get; init; }

    /// <summary>
    ///     Reserve a segment's bytes up front on a filesystem that supports it. Reserves exactly
    ///     <see cref="SegmentSize" />, so the two belong to one decision.
    /// </summary>
    public bool? PreallocateSegments { get; init; }

    /// <summary>
    ///     Renders the options that were set, each under the kind the server's catalog gives its key.
    /// </summary>
    /// <returns>Option values keyed by option name, including both durability defaults.</returns>
    public Dictionary<string, HeaderValue> ToDictionary()
    {
        var options = new Dictionary<string, HeaderValue>();

        if (SegmentSize is { } segmentSize)
        {
            options[SegmentSizeKey] = HeaderValue.FromUInt64(segmentSize);
        }

        options[DurabilityKey] = HeaderValue.FromString(EncodeDurability(Durability));
        options[ConsumerOffsetDurabilityKey] = HeaderValue.FromString(EncodeDurability(ConsumerOffsetDurability));

        if (MessagesRequiredToSave is { } messagesRequiredToSave)
        {
            options[MessagesRequiredToSaveKey] = HeaderValue.FromUInt32(messagesRequiredToSave);
        }

        if (SizeOfMessagesRequiredToSave is { } sizeOfMessagesRequiredToSave)
        {
            options[SizeOfMessagesRequiredToSaveKey] = HeaderValue.FromUInt64(sizeOfMessagesRequiredToSave);
        }

        if (PreallocateSegments is { } preallocateSegments)
        {
            options[PreallocateSegmentsKey] = HeaderValue.FromBool(preallocateSegments);
        }

        return options;
    }
    internal static Dictionary<string, HeaderValue> WithDurabilityDefaults(IReadOnlyDictionary<string, HeaderValue>? source)
    {
        var options = source is null ? new Dictionary<string, HeaderValue>() : new Dictionary<string, HeaderValue>(source);
        foreach (var key in new[] { DurabilityKey, ConsumerOffsetDurabilityKey })
        {
            if (!options.TryGetValue(key, out var value))
            {
                options[key] = HeaderValue.FromString("replicated");
            }
            else if (value.Kind != HeaderKind.String || value.ToString() is not ("replicated" or "persisted"))
            {
                throw new ArgumentException($"Invalid {key}", nameof(source));
            }
        }
        return options;
    }

    private static string EncodeDurability(Durability value) => value switch
    {
        Durability.Replicated => "replicated",
        Durability.Persisted => "persisted",
        _ => throw new ArgumentOutOfRangeException(nameof(value), value, "Unknown durability")
    };
}
