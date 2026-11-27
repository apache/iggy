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
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Utils;

namespace Apache.Iggy.Contracts;

/// <summary>
///     The changes <see cref="IIggyTopic.UpdateTopicAsync" /> applies to a topic. Every property left null keeps the
///     topic's stored value. Settings fixed at creation, compression among them, have no property here.
/// </summary>
public sealed class TopicUpdate
{
    private static readonly HashSet<TopicOptionKey> NoReset = [];

    /// <summary>The topic's new name, unique within its stream.</summary>
    public string? Name { get; init; }

    /// <summary>
    ///     The new message expiry. <see cref="TimeSpan.MaxValue" /> never expires, zero changes nothing. Reset
    ///     <see cref="TopicOptionKey.MessageExpiry" /> to return to the default.
    /// </summary>
    public TimeSpan? MessageExpiry { get; init; }

    /// <summary>
    ///     The new topic-wide size cap in bytes, at least the segment size. <see cref="ulong.MaxValue" /> is
    ///     unlimited, zero changes nothing. Reset <see cref="TopicOptionKey.MaxTopicSize" /> to return to the default.
    /// </summary>
    public ulong? MaxTopicSize { get; init; }

    /// <summary>
    ///     Turns tiered storage on. It never turns off again, so <c>false</c> is refused, before anything is sent
    ///     unless the update carries a raw key. Null changes nothing.
    /// </summary>
    /// <remarks>
    ///     Needs an object store on every node of the cluster: while one is not ready, the server refuses
    ///     <c>true</c> with <c>feature_unavailable</c>.
    /// </remarks>
    public bool? TieredStorage { get; init; }

    /// <summary>
    ///     How long a tiered topic keeps an uploaded segment on local disk, at most a finite message expiry.
    ///     <see cref="TimeSpan.MaxValue" /> never removes it, zero changes nothing. Reset
    ///     <see cref="TopicOptionKey.LocalRetentionAge" /> to return to the default.
    /// </summary>
    public TimeSpan? LocalRetentionAge { get; init; }

    /// <summary>
    ///     The new topic-wide cap in bytes on the uploaded segments a tiered topic keeps on local disk, at most a
    ///     finite topic size cap. <see cref="ulong.MaxValue" /> is unlimited, zero changes nothing. Reset
    ///     <see cref="TopicOptionKey.LocalRetentionSize" /> to return to the default.
    /// </summary>
    public ulong? LocalRetentionSize { get; init; }

    /// <summary>
    ///     The longest time a send waits for the upload of its fresh object to start, at most ten seconds. Null
    ///     changes nothing.
    /// </summary>
    /// <remarks>
    ///     This and the five durations below are the only settings an update changes on a geo topic, besides
    ///     <see cref="ReadCacheSize" /> and <see cref="ReadAhead" />, and the server refuses them on a normal topic.
    ///     Zero is a value for them and is sent, and a negative span is refused before anything is sent. They have no
    ///     default, so the server refuses a reset of them.
    /// </remarks>
    public TimeSpan? FreshUploadInterval { get; init; }

    /// <summary>
    ///     The shortest time between two uploads of the positions that the offsets its consumer groups store
    ///     themselves wait for, at most one second. Zero answers such a store at its local commit.
    /// </summary>
    public TimeSpan? ConsumerOffsetUploadInterval { get; init; }

    /// <summary>How often a node uploads the changed positions of the topic's consumer groups, more than zero.</summary>
    public TimeSpan? PositionsPublishInterval { get; init; }

    /// <summary>How long the tombstone and the objects of the deleted topic stay in the object store, more than zero.</summary>
    public TimeSpan? TombstoneGrace { get; init; }

    /// <summary>
    ///     How long, after a node starts, the group polls of the topic's partitions wait for the positions of the geo
    ///     peers. Zero does not wait.
    /// </summary>
    public TimeSpan? PositionsResumeTimeout { get; init; }

    /// <summary>The longest wait between two long-term rewrites of a partition, at least one second.</summary>
    public TimeSpan? LongTermRewriteMaxInterval { get; init; }

    /// <summary>
    ///     The new memory in bytes that each node keeps for what the topic's reads fetch from the object store. Zero
    ///     keeps nothing that they fetch, otherwise at least 512 KiB. Null changes nothing.
    /// </summary>
    /// <remarks>
    ///     This and <see cref="ReadAhead" /> change on a normal or a geo topic. Zero is a value for them and is sent,
    ///     unlike a zero <see cref="MaxTopicSize" />. They have no default, so the server refuses a reset of them.
    /// </remarks>
    public ulong? ReadCacheSize { get; init; }

    /// <summary>
    ///     The new most bytes that readahead fetches in front of a sequential reader of the topic. Zero turns
    ///     readahead off, otherwise at least 128 KiB, and at most an eighth of a nonzero read cache size that the
    ///     topic sets. Null changes nothing.
    /// </summary>
    public ulong? ReadAhead { get; init; }

    /// <summary>
    ///     Keys to reset to their defaults: <see cref="TopicOptionKey.MessageExpiry" />,
    ///     <see cref="TopicOptionKey.MaxTopicSize" />, <see cref="TopicOptionKey.LocalRetentionAge" /> or
    ///     <see cref="TopicOptionKey.LocalRetentionSize" />. Any other key is refused, before anything is sent unless
    ///     the update carries a raw key.
    /// </summary>
    public IReadOnlySet<TopicOptionKey>? Reset { get; init; }

    /// <summary>
    ///     Reset names newer than this build's typed keys. The server decides whether each exists and may be reset,
    ///     so an update naming one skips the client-side checks beyond the structural ones.
    /// </summary>
    public IReadOnlySet<string>? RawReset { get; init; }

    /// <summary>Updatable keys newer than this build, keyed by option name. A property above wins on collision.</summary>
    [JsonConverter(typeof(RedactedOptionsConverter))]
    public IReadOnlyDictionary<string, HeaderValue>? Options { get; init; }

    internal OptionsPatch ToPatch()
    {
        var set = OptionKeys.Raw(Options);
        var hasRawSet = set.Count > 0;
        if (Name is not null)
        {
            set[OptionKeys.Describe(TopicOptionKey.Name).Name] = OptionKeys.Text(Name);
        }

        if (MessageExpiry is { } messageExpiry && DurationHelpers.ToDuration(messageExpiry) is not 0 and var micros)
        {
            set[OptionKeys.Describe(TopicOptionKey.MessageExpiry).Name] = HeaderValue.FromUInt64(micros);
        }

        if (MaxTopicSize is { } maxTopicSize and not 0)
        {
            set[OptionKeys.Describe(TopicOptionKey.MaxTopicSize).Name] = HeaderValue.FromUInt64(maxTopicSize);
        }

        if (TieredStorage is { } tieredStorage)
        {
            set[OptionKeys.Describe(TopicOptionKey.TieredStorage).Name] = HeaderValue.FromBool(tieredStorage);
        }

        if (LocalRetentionAge is { } localRetentionAge
            && DurationHelpers.ToDuration(localRetentionAge) is not 0 and var localAgeMicros)
        {
            set[OptionKeys.Describe(TopicOptionKey.LocalRetentionAge).Name] = HeaderValue.FromUInt64(localAgeMicros);
        }

        if (LocalRetentionSize is { } localRetentionSize and not 0)
        {
            set[OptionKeys.Describe(TopicOptionKey.LocalRetentionSize).Name] =
                HeaderValue.FromUInt64(localRetentionSize);
        }

        SetGeoDuration(set, TopicOptionKey.FreshUploadInterval, FreshUploadInterval, nameof(FreshUploadInterval));
        SetGeoDuration(set, TopicOptionKey.ConsumerOffsetUploadInterval, ConsumerOffsetUploadInterval,
            nameof(ConsumerOffsetUploadInterval));
        SetGeoDuration(set, TopicOptionKey.PositionsPublishInterval, PositionsPublishInterval,
            nameof(PositionsPublishInterval));
        SetGeoDuration(set, TopicOptionKey.TombstoneGrace, TombstoneGrace, nameof(TombstoneGrace));
        SetGeoDuration(set, TopicOptionKey.PositionsResumeTimeout, PositionsResumeTimeout,
            nameof(PositionsResumeTimeout));
        SetGeoDuration(set, TopicOptionKey.LongTermRewriteMaxInterval, LongTermRewriteMaxInterval,
            nameof(LongTermRewriteMaxInterval));

        if (ReadCacheSize is { } readCacheSize)
        {
            set[OptionKeys.Describe(TopicOptionKey.ReadCacheSize).Name] = HeaderValue.FromUInt64(readCacheSize);
        }

        if (ReadAhead is { } readAhead)
        {
            set[OptionKeys.Describe(TopicOptionKey.ReadAhead).Name] = HeaderValue.FromUInt64(readAhead);
        }

        return OptionKeys.Patch(OptionsScope.Topic, set, (Reset ?? NoReset).Select(OptionKeys.Describe),
            RawReset, hasRawSet);
    }

    private static void SetGeoDuration(Dictionary<string, HeaderValue> set, TopicOptionKey key, TimeSpan? duration,
        string property)
    {
        if (duration is { } value)
        {
            set[OptionKeys.Describe(key).Name] = OptionKeys.GeoDuration(value, property);
        }
    }
}
