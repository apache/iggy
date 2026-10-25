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
    ///     Keys to reset to their defaults: <see cref="TopicOptionKey.MessageExpiry" /> or
    ///     <see cref="TopicOptionKey.MaxTopicSize" />. Any other key is refused, before anything is sent unless
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

        return OptionKeys.Patch(OptionsScope.Topic, set, (Reset ?? NoReset).Select(OptionKeys.Describe),
            RawReset, hasRawSet);
    }
}
