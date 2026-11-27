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

using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Headers;

namespace Apache.Iggy.Tests.UtilityTests;

public sealed class TopicOptionsTests
{
    [Fact]
    public void ToDictionary_EmitsEveryKeyUnderItsCatalogKind()
    {
        var options = new TopicOptions
        {
            SegmentSize = 134217728,
            Durability = Apache.Iggy.Enums.Durability.Persisted,
            MessagesRequiredToSave = 1024,
            SizeOfMessagesRequiredToSave = 1048576,
            PreallocateSegments = false,
            ReplicationFactor = 3,
            SegmentMaxAge = TimeSpan.FromHours(1),
            TieredStorage = false,
            LocalRetentionAge = TimeSpan.FromHours(6),
            LocalRetentionSize = 536870912,
            TopicType = TopicType.Geo,
            GeoMode = GeoMode.GeoTiered,
            FreshUploadInterval = TimeSpan.FromMilliseconds(50),
            ConsumerOffsetUploadInterval = TimeSpan.FromMilliseconds(20),
            PositionsPublishInterval = TimeSpan.FromSeconds(2),
            TombstoneGrace = TimeSpan.FromHours(1),
            PositionsResumeTimeout = TimeSpan.FromSeconds(30),
            LongTermRewriteMaxInterval = TimeSpan.FromSeconds(20),
            ReadCacheSize = 16_777_216,
            ReadAhead = 1_048_576
        }.ToDictionary();

        Assert.Equal(21, options.Count);

        Assert.Equal(HeaderKind.Uint64, options["segment_size"].Kind);
        Assert.Equal(134217728UL, options["segment_size"].ToUInt64());

        Assert.Equal(HeaderKind.String, options["durability"].Kind);
        Assert.Equal("persisted", options["durability"].ToString());

        Assert.Equal(HeaderKind.Uint32, options["messages_required_to_save"].Kind);
        Assert.Equal(1024U, options["messages_required_to_save"].ToUInt32());

        Assert.Equal(HeaderKind.Uint64, options["size_of_messages_required_to_save"].Kind);
        Assert.Equal(1048576UL, options["size_of_messages_required_to_save"].ToUInt64());

        Assert.Equal(HeaderKind.Bool, options["preallocate_segments"].Kind);
        Assert.False(options["preallocate_segments"].ToBool());

        Assert.Equal(HeaderKind.Uint32, options["replication_factor"].Kind);
        Assert.Equal(3U, options["replication_factor"].ToUInt32());

        Assert.Equal(HeaderKind.Uint64, options["segment_max_age"].Kind);
        Assert.Equal(3_600_000_000UL, options["segment_max_age"].ToUInt64());

        Assert.Equal(HeaderKind.Bool, options["tiered_storage"].Kind);
        Assert.False(options["tiered_storage"].ToBool());

        Assert.Equal(HeaderKind.Uint64, options["local_retention_age"].Kind);
        Assert.Equal(21_600_000_000UL, options["local_retention_age"].ToUInt64());

        Assert.Equal(HeaderKind.Uint64, options["local_retention_size"].Kind);
        Assert.Equal(536870912UL, options["local_retention_size"].ToUInt64());

        Assert.Equal(HeaderKind.String, options["topic_type"].Kind);
        Assert.Equal("geo", options["topic_type"].ToString());

        Assert.Equal(HeaderKind.String, options["geo_mode"].Kind);
        Assert.Equal("geo_tiered", options["geo_mode"].ToString());

        var durations = new Dictionary<string, ulong>
        {
            ["fresh_upload_interval"] = 50_000,
            ["consumer_offset_upload_interval"] = 20_000,
            ["positions_publish_interval"] = 2_000_000,
            ["tombstone_grace"] = 3_600_000_000,
            ["positions_resume_timeout"] = 30_000_000,
            ["long_term_rewrite_max_interval"] = 20_000_000
        };
        foreach (var (key, micros) in durations)
        {
            Assert.Equal(HeaderKind.Uint64, options[key].Kind);
            Assert.Equal(micros, options[key].ToUInt64());
        }

        Assert.Equal(HeaderKind.Uint64, options["read_cache_size"].Kind);
        Assert.Equal(16_777_216UL, options["read_cache_size"].ToUInt64());

        Assert.Equal(HeaderKind.Uint64, options["read_ahead"].Kind);
        Assert.Equal(1_048_576UL, options["read_ahead"].ToUInt64());
    }

    [Fact]
    public void ToDictionary_SendsAZeroReadCacheAndLeavesAnUnsetReadAheadOut()
    {
        var options = new TopicOptions { ReadCacheSize = 0, MaxTopicSize = 0 }.ToDictionary();

        // A zero size cap keeps the server default, while a zero read cache is a value.
        Assert.Equal(["consumer_offset_durability", "durability", "read_cache_size"],
            options.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(HeaderKind.Uint64, options["read_cache_size"].Kind);
        Assert.Equal(0UL, options["read_cache_size"].ToUInt64());
    }

    [Fact]
    public void ToDictionary_SendsAZeroGeoDurationAndLeavesAnUnsetOneOut()
    {
        var options = new TopicOptions
        {
            TopicType = TopicType.Geo,
            FreshUploadInterval = TimeSpan.Zero,
            PositionsResumeTimeout = TimeSpan.Zero,
            MessageExpiry = TimeSpan.Zero
        }.ToDictionary();

        // A zero expiry keeps the server default, while a zero duration of a geo topic is a value.
        Assert.Equal(
            ["consumer_offset_durability", "durability", "fresh_upload_interval", "positions_resume_timeout", "topic_type"],
            options.Keys.Order(StringComparer.Ordinal));
        Assert.Equal(HeaderKind.Uint64, options["fresh_upload_interval"].Kind);
        Assert.Equal(0UL, options["fresh_upload_interval"].ToUInt64());
        Assert.Equal(0UL, options["positions_resume_timeout"].ToUInt64());
    }

    [Fact]
    public void ToDictionary_RefusesANegativeGeoDuration()
    {
        var refused = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new TopicOptions { PositionsResumeTimeout = TimeSpan.FromMilliseconds(-1) }.ToDictionary());

        Assert.Equal(nameof(TopicOptions.PositionsResumeTimeout), refused.ParamName);
    }

    [Theory]
    [InlineData(TopicType.Normal, GeoMode.Geo, "normal", "geo")]
    [InlineData(TopicType.Geo, GeoMode.GeoTiered, "geo", "geo_tiered")]
    public void ToDictionary_WritesTopicTypeAndGeoModeUnderTheirCanonicalNames(TopicType topicType, GeoMode geoMode,
        string topicTypeName, string geoModeName)
    {
        var options = new TopicOptions { TopicType = topicType, GeoMode = geoMode }.ToDictionary();

        Assert.Equal(topicTypeName, options["topic_type"].ToString());
        Assert.Equal(geoModeName, options["geo_mode"].ToString());
    }

    [Fact]
    public void ToDictionary_EmitsOnlyTheKeysThatWereSet()
    {
        var options = new TopicOptions { Durability = Apache.Iggy.Enums.Durability.Replicated }.ToDictionary();

        Assert.Equal(2, options.Count);
        Assert.Equal("replicated", options["durability"].ToString());
        Assert.Equal("replicated", options["consumer_offset_durability"].ToString());
    }

    [Fact]
    public void ToDictionary_EmitsBothReplicatedDefaults()
    {
        Assert.Equal(2, new TopicOptions().ToDictionary().Count);
    }
}
