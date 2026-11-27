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
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     Information about a topic.
/// </summary>
public sealed class TopicResponse
{
    /// <summary>
    ///     Topic identifier.
    /// </summary>
    public required uint Id { get; init; }

    /// <summary>
    ///     Topic creation date.
    /// </summary>
    [JsonConverter(typeof(DateTimeOffsetConverter))]
    public required DateTimeOffset CreatedAt { get; init; }

    /// <summary>
    ///     Unique topic name.
    /// </summary>
    public required string Name { get; init; }

    /// <summary>
    ///     Compression algorithm label stored with the topic, read from its options.
    /// </summary>
    [JsonIgnore]
    public CompressionAlgorithm CompressionAlgorithm =>
        OptionValues.Find(Options, DerivedOptions, TopicOptionKey.CompressionAlgorithm) switch
        {
            { Kind: HeaderKind.String } value when value.ToString() == "gzip" => CompressionAlgorithm.Gzip,
            { Kind: HeaderKind.Uint8 } value => (CompressionAlgorithm)value.Value[0],
            _ => CompressionAlgorithm.None
        };

    /// <summary>
    ///     Topic size in bytes.
    /// </summary>
    [JsonConverter(typeof(SizeConverter))]
    public required ulong Size { get; init; }

    /// <summary>
    ///     How long a message is kept, read from the topic's options. <see cref="TimeSpan.MaxValue" /> never expires.
    /// </summary>
    [JsonIgnore]
    public TimeSpan MessageExpiry =>
        DurationHelpers.FromDuration(OptionValues.UInt64(Options, DerivedOptions, TopicOptionKey.MessageExpiry));

    /// <summary>
    ///     Topic-wide size cap in bytes, read from the topic's options. <see cref="ulong.MaxValue" /> is unlimited.
    /// </summary>
    [JsonIgnore]
    public ulong MaxTopicSize => OptionValues.UInt64(Options, DerivedOptions, TopicOptionKey.MaxTopicSize);

    /// <summary>
    ///     How long after the active segment's first batch a batch starts the next segment, read from the topic's
    ///     options. <see cref="TimeSpan.MaxValue" /> never rolls by age.
    /// </summary>
    [JsonIgnore]
    public TimeSpan SegmentMaxAge =>
        DurationHelpers.FromDuration(OptionValues.UInt64(Options, DerivedOptions, TopicOptionKey.SegmentMaxAge));

    /// <summary>
    ///     Whether sealed segments move to the object store, read from the topic's options. False when the server
    ///     reports no such option.
    /// </summary>
    [JsonIgnore]
    public bool TieredStorage =>
        OptionValues.Find(Options, DerivedOptions, TopicOptionKey.TieredStorage) is { Kind: HeaderKind.Bool } value
        && value.ToBool();

    /// <summary>
    ///     How long a tiered topic keeps an uploaded segment on local disk, read from the topic's options.
    ///     <see cref="TimeSpan.MaxValue" /> never removes it.
    /// </summary>
    [JsonIgnore]
    public TimeSpan LocalRetentionAge =>
        DurationHelpers.FromDuration(OptionValues.UInt64(Options, DerivedOptions, TopicOptionKey.LocalRetentionAge));

    /// <summary>
    ///     Topic-wide cap in bytes on the uploaded segments a tiered topic keeps on local disk, read from the
    ///     topic's options. <see cref="ulong.MaxValue" /> is unlimited.
    /// </summary>
    [JsonIgnore]
    public ulong LocalRetentionSize => OptionValues.UInt64(Options, DerivedOptions, TopicOptionKey.LocalRetentionSize);

    /// <summary>
    ///     Whether the topic is normal or geo, read from the topic's options. <see cref="Enums.TopicType.Normal" />
    ///     when the server reports no type.
    /// </summary>
    [JsonIgnore]
    public TopicType TopicType =>
        OptionValues.Find(Options, DerivedOptions, TopicOptionKey.TopicType) is { Kind: HeaderKind.String } value
        && value.ToString() == "geo"
            ? Enums.TopicType.Geo
            : Enums.TopicType.Normal;

    /// <summary>
    ///     How a geo topic writes its messages, read from the topic's options. Null on a normal topic.
    /// </summary>
    [JsonIgnore]
    public GeoMode? GeoMode =>
        OptionValues.Find(Options, DerivedOptions, TopicOptionKey.GeoMode) is { Kind: HeaderKind.String } value
            ? value.ToString() switch
            {
                "geo" => Enums.GeoMode.Geo,
                "geo_tiered" => Enums.GeoMode.GeoTiered,
                _ => null
            }
            : null;

    /// <summary>
    ///     The longest time a send waits for the upload of its fresh object to start, read from the topic's options.
    ///     This and the five durations below are null unless a geo topic sets them, and each node then applies its
    ///     server setting of the same name.
    /// </summary>
    [JsonIgnore]
    public TimeSpan? FreshUploadInterval =>
        OptionValues.Duration(Options, DerivedOptions, TopicOptionKey.FreshUploadInterval);

    /// <summary>
    ///     The shortest time between two uploads of the positions that the offsets its consumer groups store
    ///     themselves wait for, read from the topic's options.
    /// </summary>
    [JsonIgnore]
    public TimeSpan? ConsumerOffsetUploadInterval =>
        OptionValues.Duration(Options, DerivedOptions, TopicOptionKey.ConsumerOffsetUploadInterval);

    /// <summary>How often a node uploads the changed positions of the topic's consumer groups, read from its options.</summary>
    [JsonIgnore]
    public TimeSpan? PositionsPublishInterval =>
        OptionValues.Duration(Options, DerivedOptions, TopicOptionKey.PositionsPublishInterval);

    /// <summary>
    ///     How long the tombstone and the objects of the deleted topic stay in the object store, read from the
    ///     topic's options.
    /// </summary>
    [JsonIgnore]
    public TimeSpan? TombstoneGrace => OptionValues.Duration(Options, DerivedOptions, TopicOptionKey.TombstoneGrace);

    /// <summary>
    ///     How long, after a node starts, the group polls of the topic's partitions wait for the positions of the geo
    ///     peers, read from the topic's options.
    /// </summary>
    [JsonIgnore]
    public TimeSpan? PositionsResumeTimeout =>
        OptionValues.Duration(Options, DerivedOptions, TopicOptionKey.PositionsResumeTimeout);

    /// <summary>The longest wait between two long-term rewrites of a partition, read from the topic's options.</summary>
    [JsonIgnore]
    public TimeSpan? LongTermRewriteMaxInterval =>
        OptionValues.Duration(Options, DerivedOptions, TopicOptionKey.LongTermRewriteMaxInterval);

    /// <summary>
    ///     The memory in bytes that each node keeps for what the topic's reads fetch from the object store, read from
    ///     the topic's options. Zero keeps nothing. This and <see cref="ReadAhead" /> are null unless the topic sets
    ///     them, and each node then applies its server setting of the same name.
    /// </summary>
    [JsonIgnore]
    public ulong? ReadCacheSize => OptionValues.Size(Options, DerivedOptions, TopicOptionKey.ReadCacheSize);

    /// <summary>
    ///     The most bytes that readahead fetches in front of a sequential reader of the topic, read from its options.
    ///     Zero turns readahead off.
    /// </summary>
    [JsonIgnore]
    public ulong? ReadAhead => OptionValues.Size(Options, DerivedOptions, TopicOptionKey.ReadAhead);

    /// <summary>
    ///     Number of messages in the topic.
    /// </summary>
    public required ulong MessagesCount { get; init; }

    /// <summary>
    ///     Number of partitions in the topic.
    /// </summary>
    public required uint PartitionsCount { get; init; }


    /// <summary>
    ///     List of partitions in the topic.
    /// </summary>
    public IEnumerable<PartitionResponse>? Partitions { get; init; }

    /// <summary>
    ///     Options the client set, at creation or through an update, each in the server's canonical kind for
    ///     its key on every transport.
    /// </summary>
    [JsonIgnore]
    public Dictionary<HeaderKey, HeaderValue>? Options { get; init; }

    /// <summary>
    ///     Options resolved from server defaults, at creation or by a reset, each in the server's canonical
    ///     kind for its key on every transport.
    /// </summary>
    [JsonIgnore]
    public Dictionary<HeaderKey, HeaderValue>? DerivedOptions { get; init; }

    /// <summary>
    ///     REST renders both provenances into one object keyed by option name, so one JSON property
    ///     fills <see cref="Options" /> and <see cref="DerivedOptions" />. Those two stay ignored by
    ///     the serializer: a <see cref="HeaderKey" /> is not a JSON string key, and one of them would
    ///     claim this property's name.
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
