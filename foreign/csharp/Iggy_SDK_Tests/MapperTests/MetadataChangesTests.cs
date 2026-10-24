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

using System.Globalization;
using System.Text.Json;
using System.Text.Json.Nodes;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Headers;
using Apache.Iggy.JsonConverters;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.MapperTests;

/// <summary>
///     The cross-SDK metadata changes contract. Every expectation comes from the shared fixture the Rust encoders
///     generate, and both the binary decoder and the HTTP JSON decoder must reach the JSON the server renders.
/// </summary>
public sealed class MetadataChangesTests
{
    [Fact]
    public void given_many_duplicate_groups_when_encoding_should_preserve_every_requested_group()
    {
        var query = new MetadataChangesQuery
        {
            Groups = Enumerable.Repeat(MetadataChangeGroup.Streams, 255).Append(MetadataChangeGroup.Users).ToArray()
        };
        var expected = new MetadataChangesQuery { Groups = [MetadataChangeGroup.Streams, MetadataChangeGroup.Users] };
        Assert.Equal(TcpContracts.GetMetadataChanges(expected), TcpContracts.GetMetadataChanges(query));
    }

    internal static readonly JsonElement Fixture = LoadFixture();

    private static readonly JsonSerializerOptions PermissionsJson = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.SnakeCaseLower
    };

    public static TheoryData<string, string> SharedVectors
    {
        get
        {
            var vectors = new TheoryData<string, string>
            {
                { "constants", "constants" },
                { "large_values", "large_values" },
                { "refused_requests", "non_canonical_cursor" }
            };
            foreach (var section in new[] { "records", "items", "cursors", "page_tokens", "requests", "responses" })
            {
                AddCases(vectors, Fixture.GetProperty(section), section);
            }

            foreach (var section in new[] { "cursors", "page_tokens", "record_bodies", "responses", "queries" })
            {
                AddCases(vectors, Fixture.GetProperty("refused").GetProperty(section), $"refused_{section}");
            }

            return vectors;
        }
    }

    public static TheoryData<string> RecordCases => Cases(Fixture.GetProperty("records"));

    public static TheoryData<string> ItemCases => Cases(Fixture.GetProperty("items"));

    [Theory]
    [MemberData(nameof(SharedVectors))]
    public void given_shared_vectors_when_decoding_should_match_server_contract(string section, string vectorCase)
    {
        switch (section)
        {
            case "constants":
                AssertConstants(Fixture.GetProperty("constants"));
                break;
            case "records":
                AssertRecord(Find(Fixture.GetProperty(section), vectorCase));
                break;
            case "items":
                AssertItem(Find(Fixture.GetProperty(section), vectorCase));
                break;
            case "cursors":
                AssertCursor(Find(Fixture.GetProperty(section), vectorCase));
                break;
            case "page_tokens":
                AssertPageToken(Find(Fixture.GetProperty(section), vectorCase));
                break;
            case "requests":
                AssertRequest(Find(Fixture.GetProperty(section), vectorCase));
                break;
            case "responses":
                AssertResponse(Find(Fixture.GetProperty(section), vectorCase));
                break;
            case "large_values":
                AssertLargeValues(Fixture.GetProperty(section));
                break;
            case "refused_cursors":
                var cursorText = Refused("cursors", vectorCase).GetProperty("text").GetString();
                Assert.False(MetadataChangeCursor.TryParse(cursorText, out _));
                Assert.Throws<FormatException>(() => MetadataChangeCursor.Parse(cursorText!));
                break;
            case "refused_page_tokens":
                var tokenText = Refused("page_tokens", vectorCase).GetProperty("text").GetString();
                Assert.False(MetadataCurrentPageToken.TryParse(tokenText, out _));
                Assert.Throws<FormatException>(() => MetadataCurrentPageToken.Parse(tokenText!));
                break;
            case "refused_record_bodies":
                var body = Refused("record_bodies", vectorCase);
                Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapMetadataChangeBody(
                    (byte)body.GetProperty("group").GetInt32(), (byte)body.GetProperty("kind").GetInt32(),
                    Hex(body, "body_hex"), out _));
                break;
            case "refused_responses":
                var response = Hex(Refused("responses", vectorCase), "hex");
                Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapMetadataChanges(response));
                break;
            case "refused_queries":
                AssertRefused(Query(Refused("queries", vectorCase).GetProperty("input")));
                break;
            case "refused_requests":
                // The SDK cannot express a cursor without after, an unknown start or trailing bytes. A
                // non-canonical cursor it can, and validation refuses it before anything is sent.
                var request = Hex(Refused("requests", vectorCase), "hex");
                AssertRefused(new MetadataChangesQuery
                {
                    Start = MetadataChangeStart.After(MetadataChangeCursor.Read(request.AsSpan(1)))
                });
                break;
            default:
                Assert.Fail($"Unknown fixture section {section}.");
                break;
        }
    }

    [Theory]
    [MemberData(nameof(RecordCases))]
    public void given_record_body_when_truncating_it_should_refuse_every_prefix(string vectorCase)
    {
        var record = Find(Fixture.GetProperty("records"), vectorCase);
        var group = (byte)record.GetProperty("group").GetInt32();
        var kind = (byte)record.GetProperty("kind").GetInt32();
        var body = Hex(record, "body_hex");
        for (var length = 0; length < body.Length; length++)
        {
            var prefix = body[..length];
            Assert.Throws<MalformedResponseException>(() =>
                Mappers.BinaryMapper.MapMetadataChangeBody(group, kind, prefix, out _));
        }

        Assert.Throws<MalformedResponseException>(() =>
            Mappers.BinaryMapper.MapMetadataChangeBody(group, kind, [.. body, 0], out _));
    }

    [Theory]
    [MemberData(nameof(ItemCases))]
    public void given_item_body_when_truncating_it_should_refuse_every_prefix(string vectorCase)
    {
        var item = Find(Fixture.GetProperty("items"), vectorCase);
        var group = (byte)item.GetProperty("group").GetInt32();
        var body = Hex(item, "body_hex");
        for (var length = 0; length < body.Length; length++)
        {
            var prefix = body[..length];
            Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapMetadataItemBody(group, prefix, out _));
        }

        Assert.Throws<MalformedResponseException>(() => Mappers.BinaryMapper.MapMetadataItemBody(group, [.. body, 0], out _));
    }

    [Fact]
    public void given_unknown_codes_when_decoding_a_body_should_keep_the_codes_and_raw_bytes()
    {
        byte[] raw = [0xde, 0xad, 0xbe, 0xef];

        var change = Mappers.BinaryMapper.MapMetadataChangeBody(9, 1, raw, out var kind);
        var item = Mappers.BinaryMapper.MapMetadataItemBody(128, raw, out var group);

        Assert.Null(kind);
        Assert.Null(group);
        var unknownChange = Assert.IsType<MetadataUnknownBody>(change);
        Assert.Equal((byte)9, unknownChange.Group);
        Assert.Equal((byte)1, unknownChange.Kind);
        Assert.Equal(raw, unknownChange.Body);
        var unknownItem = Assert.IsType<MetadataUnknownBody>(item);
        Assert.Equal((byte)128, unknownItem.Group);
        Assert.Null(unknownItem.Kind);
        Assert.Equal(raw, unknownItem.Body);
    }

    [Fact]
    public void given_cursor_text_with_nonzero_trailing_bits_when_parsing_should_refuse()
    {
        var maximum = Find(Fixture.GetProperty("cursors"), "every_field_at_maximum").GetProperty("text").GetString()!;
        Assert.True(MetadataChangeCursor.TryParse(maximum, out _));

        Assert.False(MetadataChangeCursor.TryParse(maximum[..^1] + "9", out _));
        Assert.False(MetadataChangeCursor.TryParse(maximum[..^1] + " ", out _));
        Assert.False(MetadataChangeCursor.TryParse(null, out _));
    }

    [Fact]
    public void given_page_token_text_above_the_longest_token_when_parsing_should_refuse()
    {
        var longest = Find(Fixture.GetProperty("page_tokens"), "longest_token_name").GetProperty("text").GetString()!;
        Assert.True(MetadataCurrentPageToken.TryParse(longest, out var token));
        Assert.Equal(MetadataCurrentPageToken.MAX_SIZE, token.Payload.Length);

        Assert.False(MetadataCurrentPageToken.TryParse(longest + "AAAA", out _));
    }

    [Fact]
    public void given_queries_the_rules_allow_when_validating_should_accept_them()
    {
        var topicsToken = MetadataCurrentPageToken.Parse(
            Find(Fixture.GetProperty("page_tokens"), "topics").GetProperty("text").GetString()!);
        MetadataChangesQuery[] accepted =
        [
            new(),
            new() { Start = MetadataChangeStart.Latest, Limit = 1 },
            new() { Since = 5, Until = 5, Limit = MetadataChangesQuery.MAX_LIMIT },
            new() { Start = MetadataChangeStart.Current(), Groups = [MetadataChangeGroup.Sessions, MetadataChangeGroup.Users] },
            new()
            {
                Start = MetadataChangeStart.Current(topicsToken),
                Groups = [MetadataChangeGroup.Topics, MetadataChangeGroup.Streams, MetadataChangeGroup.Topics]
            },
            new() { Start = MetadataChangeStart.After(new MetadataChangeCursor(7UL, 0, UInt128.Zero, 0)), Groups = [MetadataChangeGroup.Topics] }
        ];

        foreach (var query in accepted)
        {
            query.Validate();
        }
    }

    [Fact]
    public void given_group_this_client_does_not_know_when_validating_should_refuse()
    {
        AssertRefused(new MetadataChangesQuery { Groups = [(MetadataChangeGroup)9] });
        AssertRefused(new MetadataChangesQuery { Groups = [(MetadataChangeGroup)0] });
    }

    [Fact]
    public void given_groups_in_any_order_when_encoding_should_keep_binary_order_and_sort_the_http_names()
    {
        var query = new MetadataChangesQuery
        {
            Groups = [MetadataChangeGroup.Users, MetadataChangeGroup.Streams, MetadataChangeGroup.Users]
        };

        var request = TcpContracts.GetMetadataChanges(query);

        Assert.Equal(new byte[] { 3, 6, 1, 6, 0, 0 }, request[^6..]);
        Assert.Equal("from=earliest&groups=streams,users&limit=100", query.ToHttpQuery());
    }

    private static void AssertConstants(JsonElement constants)
    {
        Assert.Equal(CommandCodes.GET_METADATA_CHANGES_CODE, constants.GetProperty("command_code").GetInt32());
        Assert.Equal(MetadataChangeCursor.PREFIX, constants.GetProperty("cursor_prefix").GetString());
        Assert.Equal(MetadataCurrentPageToken.PREFIX, constants.GetProperty("page_token_prefix").GetString());
        Assert.Equal(MetadataChangeCursor.SIZE, constants.GetProperty("cursor_size").GetInt32());
        Assert.Equal(constants.GetProperty("cursor_text_length").GetInt32(), default(MetadataChangeCursor).ToString().Length);
        Assert.Equal(MetadataCurrentPageToken.MAX_SIZE, constants.GetProperty("max_page_token_size").GetInt32());
        Assert.Equal(MetadataCurrentPageToken.MAX_KEY_SIZE, constants.GetProperty("max_page_token_key_size").GetInt32());
        Assert.Equal(MetadataChangesQuery.DEFAULT_LIMIT, constants.GetProperty("default_limit").GetUInt32());
        Assert.Equal(MetadataChangesQuery.MAX_LIMIT, constants.GetProperty("max_limit").GetUInt32());
        Assert.Equal(MetadataChangesQuery.DEFAULT_LIMIT, new MetadataChangesQuery().Limit);
        Assert.Equal(MetadataChangeStartMode.Earliest, new MetadataChangesQuery().Start.Mode);

        var groups = constants.GetProperty("groups").EnumerateObject().ToList();
        Assert.Equal(Enum.GetValues<MetadataChangeGroup>().Length, groups.Count);
        foreach (var entry in groups)
        {
            Assert.True(SnakeCaseNames<MetadataChangeGroup>.TryParse(entry.Name, out var group), entry.Name);
            Assert.Equal(entry.Value.GetProperty("code").GetInt32(), (int)group);
            Assert.Equal(entry.Value.GetProperty("parents").EnumerateArray().Select(parent => parent.GetString()!),
                MetadataChangeGroups.Parents(group).Select(SnakeCaseNames<MetadataChangeGroup>.Name));
        }

        var kinds = constants.GetProperty("kinds").EnumerateObject().ToList();
        Assert.Equal(Enum.GetValues<MetadataChangeKind>().Length, kinds.Count);
        foreach (var entry in kinds)
        {
            Assert.True(SnakeCaseNames<MetadataChangeKind>.TryParse(entry.Name, out var kind), entry.Name);
            Assert.Equal(entry.Value.GetProperty("code").GetInt32(), (int)kind);
            Assert.Equal(entry.Value.GetProperty("group").GetString(),
                SnakeCaseNames<MetadataChangeGroup>.Name(MetadataChangeGroups.Of(kind)));
        }

        AssertCodes<MetadataChangeStatus>(constants.GetProperty("statuses"));
        AssertCodes<MetadataChangePhase>(constants.GetProperty("phases"));
        AssertCodes<MetadataChangeStartMode>(constants.GetProperty("start_modes"));
        AssertCodes<MetadataMemberLeftReason>(constants.GetProperty("member_left_reasons"));
        AssertCodes<MetadataTokenDeletedReason>(constants.GetProperty("token_deleted_reasons"));
        AssertCodes<MetadataSessionClosedReason>(constants.GetProperty("session_closed_reasons"));
        AssertCodes<MetadataPermissionsUpdatedReason>(constants.GetProperty("permissions_updated_reasons"));
    }

    private static void AssertCodes<T>(JsonElement codes) where T : struct, Enum
    {
        var expected = codes.EnumerateObject()
            .ToDictionary(entry => entry.Name, entry => entry.Value.GetInt32());
        var actual = Enum.GetValues<T>()
            .ToDictionary(value => SnakeCase(value), value => Convert.ToInt32(value, CultureInfo.InvariantCulture));
        Assert.Equal(expected.OrderBy(entry => entry.Key), actual.OrderBy(entry => entry.Key));
    }

    private static void AssertRecord(JsonElement vector)
    {
        var expected = vector.GetProperty("expected");
        var group = (byte)vector.GetProperty("group").GetInt32();
        var kind = (byte)vector.GetProperty("kind").GetInt32();

        var body = Mappers.BinaryMapper.MapMetadataChangeBody(group, kind, Hex(vector, "body_hex"), out var knownKind);
        var record = Mappers.BinaryMapper.MapMetadataChangeRecord(Hex(vector, "record_hex"));

        Assert.Equal((MetadataChangeKind)kind, knownKind);
        Assert.Equal(body.GetType(), record.Body.GetType());
        AssertJson(expected, RenderRecord(record));
        var answer = ReadJsonAnswer(AnswerJson("changes", records: [expected]));
        AssertJson(expected, RenderRecord(Assert.Single(answer.Records)));
    }

    private static void AssertItem(JsonElement vector)
    {
        var expected = vector.GetProperty("expected");
        var group = (byte)vector.GetProperty("group").GetInt32();

        Mappers.BinaryMapper.MapMetadataItemBody(group, Hex(vector, "body_hex"), out var knownGroup);
        var item = Mappers.BinaryMapper.MapMetadataCurrentItem(Hex(vector, "item_hex"));

        Assert.Equal((MetadataChangeGroup)group, knownGroup);
        AssertJson(expected, RenderItem(item));
        var answer = ReadJsonAnswer(AnswerJson("current", items: [expected]));
        AssertJson(expected, RenderItem(Assert.Single(answer.Items)));
    }

    private static void AssertCursor(JsonElement vector)
    {
        var text = vector.GetProperty("text").GetString()!;

        var cursor = MetadataChangeCursor.Parse(text);

        Assert.Equal(vector.GetProperty("log_id").GetString(), HexText(cursor.LogId));
        Assert.Equal(vector.GetProperty("op").GetUInt64(), cursor.Op);
        Assert.Equal(vector.GetProperty("checksum").GetString(), HexText(cursor.Checksum));
        Assert.Equal(vector.GetProperty("ordinal").GetUInt32(), cursor.Ordinal);
        Assert.Equal(text, cursor.ToString());
        var bytes = new byte[MetadataChangeCursor.SIZE];
        cursor.WriteTo(bytes);
        Assert.Equal(Hex(vector, "hex"), bytes);
    }

    private static void AssertPageToken(JsonElement vector)
    {
        var text = vector.GetProperty("text").GetString()!;

        var token = MetadataCurrentPageToken.Parse(text);

        Assert.Equal(vector.GetProperty("anchor").GetString(), token.Anchor.ToString());
        Assert.Equal(vector.GetProperty("required_head").GetUInt64(), token.RequiredHead);
        Assert.Equal(vector.GetProperty("groups_mask").GetInt32(), token.GroupsMask);
        Assert.Equal(MetadataChangeGroups.FromMask(token.GroupsMask), token.Groups);
        Assert.Equal(vector.GetProperty("group").GetString(), SnakeCaseNames<MetadataChangeGroup>.Name(token.LastGroup));
        Assert.Equal(Hex(vector, "last_key_hex"), token.LastKey.ToArray());
        Assert.Equal(Hex(vector, "hex"), token.Payload.ToArray());
        Assert.Equal(text, token.ToString());
        Assert.Equal(token, MetadataCurrentPageToken.Parse(text));
    }

    private static void AssertRequest(JsonElement vector)
    {
        var query = Query(vector.GetProperty("input"));

        query.Validate();

        Assert.Equal(Hex(vector, "request_hex"), TcpContracts.GetMetadataChanges(query));
        Assert.Equal(vector.GetProperty("http_query").GetString(), query.ToHttpQuery());
    }

    private static void AssertResponse(JsonElement vector)
    {
        var expected = vector.GetProperty("expected");

        var answer = Mappers.BinaryMapper.MapMetadataChanges(Hex(vector, "response_hex"));

        AssertJson(expected, RenderAnswer(answer));
        AssertJson(expected, RenderAnswer(ReadJsonAnswer(expected.GetRawText())));
    }

    private static void AssertLargeValues(JsonElement vector)
    {
        var record = Mappers.BinaryMapper.MapMetadataChangeRecord(Hex(vector, "record_hex"));

        Assert.Equal(ParseU64(vector, "op"), record.Op);
        Assert.Equal(vector.GetProperty("ordinal").GetUInt32(), record.Ordinal);
        Assert.Equal(ParseU64(vector, "timestamp"), record.Timestamp);
        Assert.Equal(vector.GetProperty("checksum").GetString(), HexText(record.Checksum));
        var session = Assert.IsType<MetadataSession>(record.Body);
        Assert.Equal(ParseU64(vector, "user_created_at"), session.UserCreatedAt);
        Assert.Equal(ParseU64(vector, "session"), session.Session);
        Assert.Equal(vector.GetProperty("client_id").GetString(), HexText(session.ClientId));
    }

    private static void AssertRefused(MetadataChangesQuery query)
    {
        var refusal = Assert.Throws<IggyInvalidStatusCodeException>(query.Validate);
        Assert.Equal(VsrError.INVALID_COMMAND, refusal.StatusCode);
        Assert.False(refusal.FromServer);
    }

    private static MetadataChangesQuery Query(JsonElement input)
    {
        var start = input.GetProperty("start").GetString() switch
        {
            "earliest" => MetadataChangeStart.Earliest,
            "latest" => MetadataChangeStart.Latest,
            "after" => MetadataChangeStart.After(MetadataChangeCursor.Parse(input.GetProperty("cursor").GetString()!)),
            "current" => MetadataChangeStart.Current(input.GetProperty("page_token").GetString() is { } token
                ? MetadataCurrentPageToken.Parse(token)
                : null),
            var other => throw new InvalidOperationException($"Unknown start {other}.")
        };
        return new MetadataChangesQuery
        {
            Start = start,
            Groups = input.GetProperty("groups").EnumerateArray()
                .Select(name => SnakeCaseNames<MetadataChangeGroup>.TryParse(name.GetString()!, out var group)
                    ? group
                    : throw new InvalidOperationException($"Unknown group {name}."))
                .ToArray(),
            Since = OptionalU64(input, "since"),
            Until = OptionalU64(input, "until"),
            Limit = input.GetProperty("limit").GetUInt32()
        };
    }

    private static MetadataChanges ReadJsonAnswer(string json)
    {
        return JsonSerializer.Deserialize<MetadataChanges>(json)!;
    }

    private static string AnswerJson(string phase, JsonElement[]? records = null, JsonElement[]? items = null)
    {
        var answer = new JsonObject
        {
            ["status"] = "ok",
            ["phase"] = phase,
            ["log_id"] = null,
            ["oldest_op"] = 0,
            ["retained_since"] = 0,
            ["head_op"] = 0,
            ["next"] = null,
            ["has_more"] = false,
            ["records"] = new JsonArray((records ?? []).Select(record => JsonNode.Parse(record.GetRawText())).ToArray()),
            ["items"] = new JsonArray((items ?? []).Select(item => JsonNode.Parse(item.GetRawText())).ToArray()),
            ["current_page_token"] = null
        };
        return answer.ToJsonString();
    }

    private static JsonObject RenderAnswer(MetadataChanges answer)
    {
        return new JsonObject
        {
            ["status"] = SnakeCase(answer.Status),
            ["phase"] = SnakeCase(answer.Phase),
            ["log_id"] = answer.LogId is { } logId ? HexText(logId) : null,
            ["oldest_op"] = answer.OldestOp,
            ["retained_since"] = answer.RetainedSince,
            ["head_op"] = answer.HeadOp,
            ["next"] = answer.Next?.ToString(),
            ["has_more"] = answer.HasMore,
            ["records"] = new JsonArray(answer.Records.Select(record => (JsonNode)RenderRecord(record)).ToArray()),
            ["items"] = new JsonArray(answer.Items.Select(item => (JsonNode)RenderItem(item)).ToArray()),
            ["current_page_token"] = answer.CurrentPageToken?.ToString()
        };
    }

    private static JsonObject RenderRecord(MetadataChangeRecord record)
    {
        if (record.Body is MetadataUnknownJsonBody json)
        {
            return JsonNode.Parse(json.Json)!.AsObject();
        }

        var node = new JsonObject
        {
            ["op"] = record.Op,
            ["ordinal"] = record.Ordinal,
            ["checksum"] = HexText(record.Checksum),
            ["timestamp"] = record.Timestamp
        };
        if (record.ActorUserId is { } actor)
        {
            node["actor_user_id"] = actor;
        }

        if (record.Body is MetadataUnknownBody unknown)
        {
            node["group"] = unknown.Group.ToString(CultureInfo.InvariantCulture);
            node["kind"] = unknown.Kind!.Value.ToString(CultureInfo.InvariantCulture);
            node["body"] = Convert.ToBase64String(unknown.Body);
            return node;
        }

        node["group"] = SnakeCaseNames<MetadataChangeGroup>.Name(record.Group!.Value);
        node["kind"] = SnakeCaseNames<MetadataChangeKind>.Name(record.Kind!.Value);
        RenderBody(node, record.Body);
        return node;
    }

    private static JsonObject RenderItem(MetadataCurrentItem item)
    {
        switch (item.Body)
        {
            case MetadataUnknownJsonBody json:
                return JsonNode.Parse(json.Json)!.AsObject();
            case MetadataUnknownBody unknown:
                return new JsonObject
                {
                    ["group"] = unknown.Group.ToString(CultureInfo.InvariantCulture),
                    ["body"] = Convert.ToBase64String(unknown.Body)
                };
        }

        var node = new JsonObject { ["group"] = SnakeCaseNames<MetadataChangeGroup>.Name(item.Group!.Value) };
        RenderBody(node, item.Body);
        return node;
    }

    private static void RenderBody(JsonObject node, MetadataChangeBody body)
    {
        switch (body)
        {
            case MetadataStreamChange stream:
                StreamKey(node, stream.StreamId, stream.StreamCreatedOp);
                node["created_at"] = stream.CreatedAt;
                node["name"] = stream.Name;
                RenderOptions(node, stream.Options, stream.DerivedOptions, stream.OptionsUnreadable);
                break;
            case MetadataStreamDeleted deleted:
                StreamKey(node, deleted.StreamId, deleted.StreamCreatedOp);
                node["created_at"] = deleted.CreatedAt;
                node["name"] = deleted.Name;
                Optional(node, "requested_op", deleted.RequestedOp);
                break;
            case MetadataTopicChange topic:
                TopicKey(node, topic.StreamId, topic.StreamCreatedOp, topic.TopicId, topic.TopicCreatedOp);
                node["created_at"] = topic.CreatedAt;
                node["name"] = topic.Name;
                node["partitions_count"] = topic.PartitionsCount;
                RenderOptions(node, topic.Options, topic.DerivedOptions, topic.OptionsUnreadable);
                break;
            case MetadataTopicItem topic:
                TopicKey(node, topic.StreamId, topic.StreamCreatedOp, topic.TopicId, topic.TopicCreatedOp);
                node["created_at"] = topic.CreatedAt;
                node["name"] = topic.Name;
                RenderOptions(node, topic.Options, topic.DerivedOptions, topic.OptionsUnreadable);
                break;
            case MetadataTopicDeleted topic:
                TopicKey(node, topic.StreamId, topic.StreamCreatedOp, topic.TopicId, topic.TopicCreatedOp);
                node["created_at"] = topic.CreatedAt;
                node["name"] = topic.Name;
                Optional(node, "requested_op", topic.RequestedOp);
                break;
            case MetadataPartitionsChanged partitions:
                TopicKey(node, partitions.StreamId, partitions.StreamCreatedOp, partitions.TopicId,
                    partitions.TopicCreatedOp);
                node["partitions_count"] = partitions.PartitionsCount;
                node["removed_partition_ids"] = Ids(partitions.RemovedPartitionIds);
                Optional(node, "requested_op", partitions.RequestedOp);
                break;
            case MetadataPartitionsItem partitions:
                TopicKey(node, partitions.StreamId, partitions.StreamCreatedOp, partitions.TopicId,
                    partitions.TopicCreatedOp);
                node["partitions_count"] = partitions.PartitionsCount;
                break;
            case MetadataConsumerGroupCreated group:
                TopicKey(node, group.StreamId, group.StreamCreatedOp, group.TopicId, group.TopicCreatedOp);
                node["group_id"] = group.GroupId;
                node["name"] = group.Name;
                node["consumer_groups_count"] = group.ConsumerGroupsCount;
                break;
            case MetadataConsumerGroupDeleted group:
                TopicKey(node, group.StreamId, group.StreamCreatedOp, group.TopicId, group.TopicCreatedOp);
                node["group_id"] = group.GroupId;
                node["name"] = group.Name;
                node["consumer_groups_count"] = group.ConsumerGroupsCount;
                Optional(node, "requested_op", group.RequestedOp);
                break;
            case MetadataConsumerGroupItem group:
                TopicKey(node, group.StreamId, group.StreamCreatedOp, group.TopicId, group.TopicCreatedOp);
                node["group_id"] = group.GroupId;
                node["name"] = group.Name;
                break;
            case MetadataMemberJoined member:
                TopicKey(node, member.StreamId, member.StreamCreatedOp, member.TopicId, member.TopicCreatedOp);
                node["group_id"] = member.GroupId;
                node["member_id"] = member.MemberId;
                node["client_id"] = HexText(member.ClientId);
                node["members_count"] = member.MembersCount;
                node["partition_ids"] = Ids(member.PartitionIds);
                break;
            case MetadataMemberLeft member:
                TopicKey(node, member.StreamId, member.StreamCreatedOp, member.TopicId, member.TopicCreatedOp);
                node["group_id"] = member.GroupId;
                node["member_id"] = member.MemberId;
                node["client_id"] = HexText(member.ClientId);
                node["members_count"] = member.MembersCount;
                node["reason"] = SnakeCase(member.Reason);
                break;
            case MetadataPartitionHandedOver handover:
                TopicKey(node, handover.StreamId, handover.StreamCreatedOp, handover.TopicId, handover.TopicCreatedOp);
                node["group_id"] = handover.GroupId;
                node["partition_id"] = handover.PartitionId;
                Optional(node, "member_id", handover.MemberId);
                break;
            case MetadataMemberItem member:
                TopicKey(node, member.StreamId, member.StreamCreatedOp, member.TopicId, member.TopicCreatedOp);
                node["group_id"] = member.GroupId;
                node["member_id"] = member.MemberId;
                node["client_id"] = HexText(member.ClientId);
                node["partition_ids"] = Ids(member.PartitionIds);
                break;
            case MetadataUserChange user:
                UserKey(node, user.UserId, user.UserCreatedAt);
                node["username"] = user.Username;
                node["status"] = SnakeCase(user.Status);
                node["permissions"] = JsonSerializer.SerializeToNode(user.Permissions, PermissionsJson);
                RenderOptions(node, user.Options, user.DerivedOptions, user.OptionsUnreadable);
                break;
            case MetadataUserIdentity user:
                UserKey(node, user.UserId, user.UserCreatedAt);
                node["username"] = user.Username;
                break;
            case MetadataUserPermissionsUpdated user:
                UserKey(node, user.UserId, user.UserCreatedAt);
                node["username"] = user.Username;
                node["permissions"] = JsonSerializer.SerializeToNode(user.Permissions, PermissionsJson);
                node["reason"] = SnakeCase(user.Reason);
                break;
            case MetadataTokenChange token:
                UserKey(node, token.UserId, token.UserCreatedAt);
                node["name"] = token.Name;
                Optional(node, "expiry", token.Expiry);
                break;
            case MetadataTokenDeleted token:
                UserKey(node, token.UserId, token.UserCreatedAt);
                node["name"] = token.Name;
                node["reason"] = SnakeCase(token.Reason);
                break;
            case MetadataSession session:
                UserKey(node, session.UserId, session.UserCreatedAt);
                node["client_id"] = HexText(session.ClientId);
                node["session"] = session.Session;
                break;
            case MetadataSessionClosed session:
                UserKey(node, session.UserId, session.UserCreatedAt);
                node["client_id"] = HexText(session.ClientId);
                node["session"] = session.Session;
                node["reason"] = SnakeCase(session.Reason);
                break;
            default:
                Assert.Fail($"No rendering for {body.GetType().Name}.");
                break;
        }
    }

    private static void RenderOptions(JsonObject node, Dictionary<HeaderKey, HeaderValue>? options,
        Dictionary<HeaderKey, HeaderValue>? derivedOptions, bool unreadable)
    {
        if (unreadable)
        {
            Assert.Null(options);
            Assert.Null(derivedOptions);
            node["options"] = null;
            node["options_unreadable"] = true;
            return;
        }

        var rendered = new JsonObject();
        foreach (var (entries, isExplicit) in new[] { (options!, true), (derivedOptions!, false) })
        {
            foreach (var (key, value) in entries)
            {
                rendered[key.AsString()] = new JsonObject
                {
                    ["value"] = ResourceOptionsConverter.EncodeValue(value),
                    ["kind"] = value.Kind switch
                    {
                        HeaderKind.Float => "float32",
                        HeaderKind.Double => "float64",
                        var kind => kind.ToString().ToLowerInvariant()
                    },
                    ["explicit"] = isExplicit
                };
            }
        }

        node["options"] = rendered;
    }

    private static void StreamKey(JsonObject node, uint streamId, ulong streamCreatedOp)
    {
        node["stream_id"] = streamId;
        node["stream_created_op"] = streamCreatedOp;
    }

    private static void TopicKey(JsonObject node, uint streamId, ulong streamCreatedOp, uint topicId,
        ulong topicCreatedOp)
    {
        StreamKey(node, streamId, streamCreatedOp);
        node["topic_id"] = topicId;
        node["topic_created_op"] = topicCreatedOp;
    }

    private static void UserKey(JsonObject node, uint userId, ulong userCreatedAt)
    {
        node["user_id"] = userId;
        node["user_created_at"] = userCreatedAt;
    }

    private static void Optional(JsonObject node, string name, ulong? value)
    {
        if (value is { } present)
        {
            node[name] = present;
        }
    }

    private static void Optional(JsonObject node, string name, uint? value)
    {
        if (value is { } present)
        {
            node[name] = present;
        }
    }

    private static JsonArray Ids(IReadOnlyList<uint> ids)
    {
        return new JsonArray(ids.Select(id => (JsonNode)id).ToArray());
    }

    /// <summary>Compares JSON documents with object members sorted, so member order never matters.</summary>
    private static void AssertJson(JsonElement expected, JsonNode actual)
    {
        Assert.Equal(Canonical(JsonNode.Parse(expected.GetRawText())), Canonical(actual));
    }

    private static string Canonical(JsonNode? node)
    {
        return node switch
        {
            null => "null",
            JsonObject members => "{" + string.Join(",", members.OrderBy(member => member.Key, StringComparer.Ordinal)
                .Select(member => $"{JsonSerializer.Serialize(member.Key)}:{Canonical(member.Value)}")) + "}",
            JsonArray elements => "[" + string.Join(",", elements.Select(Canonical)) + "]",
            _ => node.ToJsonString()
        };
    }

    private static string SnakeCase<T>(T value) where T : struct, Enum
    {
        return JsonNamingPolicy.SnakeCaseLower.ConvertName(value.ToString());
    }

    private static string HexText(UInt128 value)
    {
        return value.ToString("x32", CultureInfo.InvariantCulture);
    }

    private static ulong? OptionalU64(JsonElement element, string property)
    {
        return element.GetProperty(property).ValueKind == JsonValueKind.Null
            ? null
            : element.GetProperty(property).GetUInt64();
    }

    private static ulong ParseU64(JsonElement element, string property)
    {
        return ulong.Parse(element.GetProperty(property).GetString()!, CultureInfo.InvariantCulture);
    }

    private static byte[] Hex(JsonElement vector, string property)
    {
        return Convert.FromHexString(vector.GetProperty(property).GetString()!);
    }

    private static JsonElement Refused(string section, string vectorCase)
    {
        return Find(Fixture.GetProperty("refused").GetProperty(section), vectorCase);
    }

    private static JsonElement Find(JsonElement section, string vectorCase)
    {
        return section.EnumerateArray().Single(entry => entry.GetProperty("case").GetString() == vectorCase);
    }

    private static void AddCases(TheoryData<string, string> vectors, JsonElement section, string name)
    {
        foreach (var entry in section.EnumerateArray())
        {
            vectors.Add(name, entry.GetProperty("case").GetString()!);
        }
    }

    private static TheoryData<string> Cases(JsonElement section)
    {
        var cases = new TheoryData<string>();
        foreach (var entry in section.EnumerateArray())
        {
            cases.Add(entry.GetProperty("case").GetString()!);
        }

        return cases;
    }

    private static JsonElement LoadFixture()
    {
        var path = Path.Combine(AppContext.BaseDirectory, "Fixtures", "metadata_changes.json");
        using var document = JsonDocument.Parse(File.ReadAllText(path));
        Assert.Equal(1, document.RootElement.GetProperty("schema_version").GetInt32());
        return document.RootElement.Clone();
    }
}
