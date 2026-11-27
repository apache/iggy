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
///     <see cref="MessageExpiry" />, <see cref="MaxTopicSize" />, <see cref="LocalRetentionAge" /> and
///     <see cref="LocalRetentionSize" /> can change later through a <see cref="TopicUpdate" />, which can also turn
///     <see cref="TieredStorage" /> on and set <see cref="ReadCacheSize" />, <see cref="ReadAhead" /> and the six
///     durations of a geo topic again. Every other setting is fixed at creation. A property left null emits no key,
///     which leaves the server default in place, or for the read cache and a duration of a geo topic the server
///     setting of each node. <see cref="IIggySystem.DescribeOptionsAsync" /> enumerates the keys a given server
///     accepts, with the kind, default and mutability of each.
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
    ///     Number of nodes that store each partition, 1 to 32 and at most the cluster's node count. Null lets the
    ///     server's <c>[cluster] default_replication_factor</c> apply: every node, unless an operator set a number,
    ///     which is lowered to the node count when the cluster is smaller. Fixed at creation.
    /// </summary>
    /// <remarks>
    ///     Two replicas keep two copies but stop accepting writes while either node is down, like Kafka with
    ///     <c>min.insync.replicas=2</c>. The server refuses an out-of-range value with error code 2018,
    ///     <c>invalid_replication_factor</c>.
    /// </remarks>
    public uint? ReplicationFactor { get; init; }

    /// <summary>
    ///     How long after the active segment's first batch a batch starts the next segment.
    ///     <see cref="TimeSpan.MaxValue" /> never rolls by age, zero keeps the server default. Fixed at creation.
    /// </summary>
    /// <remarks>
    ///     The server refuses less than ten minutes, and any age together with <see cref="PreallocateSegments" />.
    /// </remarks>
    public TimeSpan? SegmentMaxAge { get; init; }

    /// <summary>
    ///     Move sealed segments to the object store and keep only the local retention on disk. An update can turn
    ///     it on and never off.
    /// </summary>
    /// <remarks>
    ///     Needs an object store on every node of the cluster: while one is not ready, the server refuses
    ///     <c>true</c> with <c>feature_unavailable</c>.
    /// </remarks>
    public bool? TieredStorage { get; init; }

    /// <summary>
    ///     How long a tiered topic keeps an uploaded segment on local disk. <see cref="TimeSpan.MaxValue" /> never
    ///     removes it, zero keeps the server default of one day. No effect without <see cref="TieredStorage" />.
    /// </summary>
    /// <remarks>
    ///     The server refuses a value above a finite <see cref="MessageExpiry" />.
    /// </remarks>
    public TimeSpan? LocalRetentionAge { get; init; }

    /// <summary>
    ///     Topic-wide cap in bytes on the uploaded segments a tiered topic keeps on local disk, divided among
    ///     partitions. <see cref="ulong.MaxValue" /> is unlimited, zero keeps the server default. No effect without
    ///     <see cref="TieredStorage" />.
    /// </summary>
    /// <remarks>
    ///     The server refuses a value above a finite <see cref="MaxTopicSize" />.
    /// </remarks>
    public ulong? LocalRetentionSize { get; init; }

    /// <summary>
    ///     Whether the topic is normal or geo. The server default is normal. Fixed at creation.
    /// </summary>
    /// <remarks>
    ///     A geo topic needs an object store on every node of the cluster: while one is not ready, the server
    ///     refuses <see cref="Enums.TopicType.Geo" /> with <c>feature_unavailable</c>. A geo topic refuses tiered
    ///     storage and an encrypted stream, and keeps its name, options and partition count.
    /// </remarks>
    public TopicType? TopicType { get; init; }

    /// <summary>
    ///     How a geo topic writes its messages. A geo topic created without one uses
    ///     <see cref="Enums.GeoMode.Geo" />. Fixed at creation.
    /// </summary>
    /// <remarks>
    ///     The server refuses <see cref="Enums.GeoMode.GeoTiered" /> with <c>feature_unavailable</c> for now.
    ///     Only a geo topic takes a mode: the server refuses one on a normal topic with error code 4042,
    ///     <c>invalid_option_value</c>.
    /// </remarks>
    public GeoMode? GeoMode { get; init; }

    /// <summary>
    ///     The longest time a send waits for the upload of its fresh object to start, at most ten seconds. Zero
    ///     uploads as soon as an upload slot is free. Overrides the server's <c>[tiered_storage]
    ///     fresh_upload_interval</c>.
    /// </summary>
    /// <remarks>
    ///     This and the five durations below are only on a geo topic: the server refuses them on a normal topic
    ///     with error code 4042, <c>invalid_option_value</c>. Zero is a value for them and is sent, unlike a zero
    ///     <see cref="MessageExpiry" />, and a negative span is refused before anything is sent. Null leaves each
    ///     node to apply its own server setting of the same name.
    /// </remarks>
    public TimeSpan? FreshUploadInterval { get; init; }

    /// <summary>
    ///     The shortest time between two uploads of the positions that the offsets its consumer groups store
    ///     themselves wait for, at most one second. Zero answers such a store at its local commit. Overrides the
    ///     server's <c>[geo] consumer_offset_upload_interval</c>.
    /// </summary>
    public TimeSpan? ConsumerOffsetUploadInterval { get; init; }

    /// <summary>
    ///     How often a node uploads the changed positions of the topic's consumer groups, more than zero. Overrides
    ///     the server's <c>[geo] positions_publish_interval</c>.
    /// </summary>
    public TimeSpan? PositionsPublishInterval { get; init; }

    /// <summary>
    ///     How long the tombstone and the objects of the deleted topic stay in the object store, more than zero.
    ///     Overrides the server's <c>[geo] tombstone_grace</c>.
    /// </summary>
    public TimeSpan? TombstoneGrace { get; init; }

    /// <summary>
    ///     How long, after a node starts, the group polls of the topic's partitions wait for the positions of the geo
    ///     peers. Zero does not wait. Overrides the server's <c>[geo] positions_resume_timeout</c>.
    /// </summary>
    public TimeSpan? PositionsResumeTimeout { get; init; }

    /// <summary>
    ///     The longest wait between two rewrites of a partition's committed messages into long-term segments, at
    ///     least one second. Overrides the server's <c>[geo] long_term_rewrite_max_interval</c>.
    /// </summary>
    public TimeSpan? LongTermRewriteMaxInterval { get; init; }

    /// <summary>
    ///     The memory in bytes that each node keeps for what the topic's reads fetch from the object store, divided
    ///     between its partitions. Zero keeps nothing that they fetch, otherwise at least 512 KiB. Overrides the
    ///     server's <c>[tiered_storage] read_cache_size</c>.
    /// </summary>
    /// <remarks>
    ///     This and <see cref="ReadAhead" /> apply to a normal or a geo topic. Zero is a value for them and is sent,
    ///     unlike a zero <see cref="MaxTopicSize" />. Null leaves each node to apply its own server setting of the
    ///     same name. The share of a partition is a limit within the read cache of its shard, not a reservation.
    /// </remarks>
    public ulong? ReadCacheSize { get; init; }

    /// <summary>
    ///     The most bytes that readahead fetches from the object store in front of a sequential reader of the topic.
    ///     Zero turns readahead off, otherwise at least 128 KiB, and at most an eighth of a nonzero
    ///     <see cref="ReadCacheSize" />. Overrides the server's <c>[tiered_storage] read_ahead</c>.
    /// </summary>
    public ulong? ReadAhead { get; init; }

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
    /// <exception cref="ArgumentOutOfRangeException">A duration of a geo topic is negative.</exception>
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

        if (ReplicationFactor is { } replicationFactor)
        {
            options[Name(TopicOptionKey.ReplicationFactor)] = HeaderValue.FromUInt32(replicationFactor);
        }

        if (SegmentMaxAge is { } segmentMaxAge && DurationHelpers.ToDuration(segmentMaxAge) is not 0 and var ageMicros)
        {
            options[Name(TopicOptionKey.SegmentMaxAge)] = HeaderValue.FromUInt64(ageMicros);
        }

        if (TieredStorage is { } tieredStorage)
        {
            options[Name(TopicOptionKey.TieredStorage)] = HeaderValue.FromBool(tieredStorage);
        }

        if (LocalRetentionAge is { } localRetentionAge
            && DurationHelpers.ToDuration(localRetentionAge) is not 0 and var localAgeMicros)
        {
            options[Name(TopicOptionKey.LocalRetentionAge)] = HeaderValue.FromUInt64(localAgeMicros);
        }

        if (LocalRetentionSize is { } localRetentionSize and not 0)
        {
            options[Name(TopicOptionKey.LocalRetentionSize)] = HeaderValue.FromUInt64(localRetentionSize);
        }

        if (TopicType is { } topicType)
        {
            options[Name(TopicOptionKey.TopicType)] = HeaderValue.FromString(topicType switch
            {
                Enums.TopicType.Normal => "normal",
                Enums.TopicType.Geo => "geo",
                _ => throw new ArgumentOutOfRangeException(nameof(TopicType), topicType, "Unknown topic type")
            });
        }

        if (GeoMode is { } geoMode)
        {
            options[Name(TopicOptionKey.GeoMode)] = HeaderValue.FromString(geoMode switch
            {
                Enums.GeoMode.Geo => "geo",
                Enums.GeoMode.GeoTiered => "geo_tiered",
                _ => throw new ArgumentOutOfRangeException(nameof(GeoMode), geoMode, "Unknown geo mode")
            });
        }

        AddGeoDuration(options, TopicOptionKey.FreshUploadInterval, FreshUploadInterval, nameof(FreshUploadInterval));
        AddGeoDuration(options, TopicOptionKey.ConsumerOffsetUploadInterval, ConsumerOffsetUploadInterval,
            nameof(ConsumerOffsetUploadInterval));
        AddGeoDuration(options, TopicOptionKey.PositionsPublishInterval, PositionsPublishInterval,
            nameof(PositionsPublishInterval));
        AddGeoDuration(options, TopicOptionKey.TombstoneGrace, TombstoneGrace, nameof(TombstoneGrace));
        AddGeoDuration(options, TopicOptionKey.PositionsResumeTimeout, PositionsResumeTimeout,
            nameof(PositionsResumeTimeout));
        AddGeoDuration(options, TopicOptionKey.LongTermRewriteMaxInterval, LongTermRewriteMaxInterval,
            nameof(LongTermRewriteMaxInterval));

        if (ReadCacheSize is { } readCacheSize)
        {
            options[Name(TopicOptionKey.ReadCacheSize)] = HeaderValue.FromUInt64(readCacheSize);
        }

        if (ReadAhead is { } readAhead)
        {
            options[Name(TopicOptionKey.ReadAhead)] = HeaderValue.FromUInt64(readAhead);
        }

        return options;
    }

    private static void AddGeoDuration(Dictionary<string, HeaderValue> options, TopicOptionKey key,
        TimeSpan? duration, string property)
    {
        if (duration is { } value)
        {
            options[Name(key)] = OptionKeys.GeoDuration(value, property);
        }
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
