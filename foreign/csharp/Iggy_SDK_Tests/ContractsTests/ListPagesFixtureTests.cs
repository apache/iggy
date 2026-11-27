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

using System.Net;
using System.Text.Json;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Mappers;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ContractsTests;

/// <summary>
///     The list page vectors that the Rust encoder generates for every SDK: each request encodes to the same bytes and
///     HTTP query, each reply decodes to the same items and next cursor, and each refused vector is refused.
/// </summary>
public sealed class ListPagesFixtureTests
{
    /// <summary>Order id, ascending and the default limit: the page block before its cursor.</summary>
    private const string DefaultQueryHex = "000064000000";

    private const string NoNameHex = "00";

    internal static readonly JsonElement Fixture = JsonDocument
        .Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "list_pages.json")))
        .RootElement;

    private static readonly JsonElement Refused = Fixture.GetProperty("refused");

    public static TheoryData<string, string> Cursors => Cases(Fixture, "cursors", "opaque_cursors");

    public static TheoryData<string, string> Requests => Cases(Fixture, "requests");

    public static TheoryData<string, string> Responses => Cases(Fixture, "responses", "opaque_responses");

    public static TheoryData<string, string> OpaqueCursors => Cases(Fixture, "opaque_cursors");

    public static TheoryData<string, string> OpaqueResponses => Cases(Fixture, "opaque_responses");

    public static TheoryData<string, string> RefusedCursors => Cases(Refused, "cursors");

    public static TheoryData<string, string> RefusedQueries => Cases(Refused, "queries");

    public static TheoryData<string, string> RefusedResponses => Cases(Refused, "responses");

    [Fact]
    public void given_the_fixture_constants_when_compared_should_match_the_client()
    {
        var constants = Fixture.GetProperty("constants");
        var codes = constants.GetProperty("codes");
        var errors = constants.GetProperty("errors");

        Assert.Equal(
            (CommandCodes.GET_STREAMS_CODE, CommandCodes.GET_TOPICS_CODE, CommandCodes.GET_PARTITIONS_CODE,
                CommandCodes.GET_CONSUMER_GROUPS_CODE, CommandCodes.GET_USERS_CODE),
            (codes.GetProperty("get_streams").GetInt32(), codes.GetProperty("get_topics").GetInt32(),
                codes.GetProperty("get_partitions").GetInt32(), codes.GetProperty("get_consumer_groups").GetInt32(),
                codes.GetProperty("get_users").GetInt32()));
        foreach (var code in codes.EnumerateObject())
        {
            Assert.Equal(VsrOperation.NonReplicated, VsrOperations.ForCode(code.Value.GetInt32()));
            Assert.True(VsrOperations.IsReplaySafeRead(code.Value.GetInt32(), TcpContracts.GetPage(new NamedPageQuery())),
                $"{code.Name} is not a read that is safe to send again after a lost connection.");
        }

        Assert.Equal((byte)PageOrder.Id, constants.GetProperty("orders").GetProperty("id").GetByte());
        Assert.Equal((byte)PageOrder.Created, constants.GetProperty("orders").GetProperty("created").GetByte());
        Assert.Equal(PageCursor.PREFIX, constants.GetProperty("cursor_prefix").GetString());
        Assert.Equal(PageCursor.MAX_SIZE, constants.GetProperty("max_cursor_bytes").GetInt32());
        Assert.Equal(PageQuery.DEFAULT_LIMIT, constants.GetProperty("default_limit").GetUInt32());
        Assert.Equal(PageQuery.DEFAULT_LIMIT, new PageQuery().Limit);
        Assert.Equal(PageQuery.MAX_LIMIT, constants.GetProperty("max_limit").GetUInt32());
        Assert.Equal(NamedPageQuery.MAX_NAME_SIZE, constants.GetProperty("max_name_bytes").GetInt32());
        Assert.Equal(VsrError.INVALID_COMMAND, errors.GetProperty("invalid_command").GetInt32());
        Assert.Equal(VsrError.REPLY_TOO_LARGE, errors.GetProperty("reply_too_large").GetInt32());
    }

    [Theory]
    [MemberData(nameof(Cursors))]
    public void given_a_cursor_vector_when_parsed_should_round_trip_its_text_and_bytes(string group, string name)
    {
        var vector = Case(Fixture, group, name);
        var text = vector.GetProperty("text").GetString()!;
        var bytes = Convert.FromHexString(vector.GetProperty("hex").GetString()!);

        var cursor = PageCursor.Parse(text);

        Assert.Equal(text, cursor.ToString());
        Assert.Equal(bytes, cursor.Bytes.ToArray());
        Assert.Equal(cursor, PageCursor.FromBytes(bytes));
        Assert.Equal(cursor, JsonSerializer.Deserialize<PageCursor>(JsonSerializer.Serialize(text)));
        Assert.Equal(JsonSerializer.Serialize(text), JsonSerializer.Serialize(cursor));
    }

    [Theory]
    [MemberData(nameof(Requests))]
    public void given_a_request_vector_when_encoded_should_match_the_rust_encoder_and_http_query(string group,
        string name)
    {
        var vector = Case(Fixture, group, name);
        var input = vector.GetProperty("input");
        var query = vector.GetProperty("command").GetString() == "get_partitions"
            ? new NamedPageQuery { Page = Query(input) }
            : NamedQuery(input);

        query.Validate();

        Assert.Equal(vector.GetProperty("request_hex").GetString(),
            Convert.ToHexString(TcpContracts.GetPage(query, Parents(input))).ToLowerInvariant());
        Assert.Equal(
            vector.GetProperty("http_query").EnumerateObject().ToDictionary(key => key.Name,
                key => key.Value.GetString()!),
            FormDecode(query.ToHttpQuery()));
    }

    [Theory]
    [MemberData(nameof(Responses))]
    public void given_a_response_vector_when_decoded_should_keep_its_items_and_next_cursor(string group, string name)
    {
        var vector = Case(Fixture, group, name);
        var next = vector.GetProperty("next");

        var (items, cursor) = Decode(vector.GetProperty("command").GetString()!,
            Convert.FromHexString(vector.GetProperty("response_hex").GetString()!));

        Assert.Equal(
            vector.GetProperty("items").EnumerateArray()
                .Select(item => (item.GetProperty("id").GetUInt32(), item.GetProperty("name").GetString())),
            items);
        Assert.Equal(next.ValueKind == JsonValueKind.Null ? null : next.GetString(), cursor?.ToString());
    }

    /// <summary>
    ///     The client reads only the text of a cursor, so it sends back the bytes of a cursor that the server refuses.
    /// </summary>
    [Theory]
    [MemberData(nameof(OpaqueCursors))]
    public void given_an_opaque_cursor_when_sent_back_should_keep_its_bytes(string group, string name)
    {
        var vector = Case(Fixture, group, name);

        AssertSentBack(PageCursor.Parse(vector.GetProperty("text").GetString()!), vector.GetProperty("hex").GetString()!);
    }

    [Theory]
    [MemberData(nameof(OpaqueResponses))]
    public void given_an_opaque_next_when_sent_back_should_keep_its_bytes(string group, string name)
    {
        var vector = Case(Fixture, group, name);
        var reply = Convert.FromHexString(vector.GetProperty("response_hex").GetString()!);

        var (_, next) = Decode(vector.GetProperty("command").GetString()!, reply);

        AssertSentBack(next!, Convert.ToHexString(reply, 1, reply[0]).ToLowerInvariant());
    }

    [Theory]
    [MemberData(nameof(RefusedCursors))]
    public void given_a_refused_cursor_vector_when_parsed_should_refuse_it(string group, string name)
    {
        var text = Case(Refused, group, name).GetProperty("text").GetString()!;

        Assert.False(PageCursor.TryParse(text, out _));
        Assert.Throws<FormatException>(() => PageCursor.Parse(text));
        Assert.Throws<JsonException>(() => JsonSerializer.Deserialize<PageCursor>(JsonSerializer.Serialize(text)));
    }

    [Theory]
    [MemberData(nameof(RefusedQueries))]
    public void given_a_refused_query_vector_when_validated_should_refuse_it_before_sending(string group, string name)
    {
        var input = Case(Refused, group, name).GetProperty("input");

        AssertRefused(NamedQuery(input).Validate);
        if (input.GetProperty("name").ValueKind == JsonValueKind.Null)
        {
            AssertRefused(Query(input).Validate);
        }
    }

    [Fact]
    public void given_a_query_the_vectors_cannot_hold_when_validated_should_refuse_it_before_sending()
    {
        AssertRefused(new NamedPageQuery { Name = "\uD800" }.Validate);
        AssertRefused(new PageQuery { Order = (PageOrder)2 }.Validate);
        Assert.Throws<ArgumentNullException>(new NamedPageQuery { Page = null! }.Validate);
    }

    [Theory]
    [MemberData(nameof(RefusedResponses))]
    public void given_a_refused_response_vector_when_decoded_should_refuse_it_as_malformed(string group, string name)
    {
        var vector = Case(Refused, group, name);

        Assert.Throws<MalformedResponseException>(() => Decode(vector.GetProperty("command").GetString()!,
            Convert.FromHexString(vector.GetProperty("response_hex").GetString()!)));
    }

    /// <summary>A page that ends early never decodes into fewer items, and never fails with an index error.</summary>
    [Theory]
    [MemberData(nameof(Responses))]
    public void given_a_response_vector_cut_at_any_length_when_decoded_should_refuse_it_as_malformed(string group,
        string name)
    {
        var vector = Case(Fixture, group, name);
        var reply = Convert.FromHexString(vector.GetProperty("response_hex").GetString()!);

        for (var length = 0; length < reply.Length; length++)
        {
            var cut = reply[..length];
            Assert.Throws<MalformedResponseException>(() => Decode(vector.GetProperty("command").GetString()!, cut));
        }
    }

    /// <summary>The query of a vector input without its name, with the order from the fixture constants.</summary>
    internal static PageQuery Query(JsonElement input)
    {
        var cursor = input.GetProperty("cursor");

        return new PageQuery
        {
            Order = (PageOrder)Fixture.GetProperty("constants").GetProperty("orders")
                .GetProperty(input.GetProperty("order").GetString()!).GetByte(),
            Descending = input.GetProperty("descending").GetBoolean(),
            Limit = input.GetProperty("limit").GetUInt32(),
            Cursor = cursor.ValueKind == JsonValueKind.Null ? null : PageCursor.Parse(cursor.GetString()!)
        };
    }

    /// <summary>The query of a vector input of a list with a name filter.</summary>
    internal static NamedPageQuery NamedQuery(JsonElement input)
    {
        var name = input.GetProperty("name");

        return new NamedPageQuery
        {
            Page = Query(input),
            Name = name.ValueKind == JsonValueKind.Null ? null : name.GetString()
        };
    }

    internal static Identifier[] Parents(JsonElement input)
    {
        return
        [
            .. new[] { input.GetProperty("stream_id"), input.GetProperty("topic_id") }
                .Where(identifier => identifier.ValueKind != JsonValueKind.Null)
                .Select(identifier => identifier.GetProperty("kind").GetString() == "numeric"
                    ? Identifier.Numeric(identifier.GetProperty("value").GetUInt32())
                    : Identifier.String(identifier.GetProperty("value").GetString()!))
        ];
    }

    /// <summary>Reads a query string the way the server reads a form: a plus is a space, then percent decoding.</summary>
    internal static Dictionary<string, string> FormDecode(string query)
    {
        var pairs = new Dictionary<string, string>();
        foreach (var pair in query.Split('&'))
        {
            var separator = pair.IndexOf('=');
            pairs.Add(WebUtility.UrlDecode(pair[..separator]), WebUtility.UrlDecode(pair[(separator + 1)..]));
        }

        return pairs;
    }

    internal static JsonElement Case(JsonElement parent, string group, string name)
    {
        return parent.GetProperty(group).EnumerateArray()
            .Single(vector => vector.GetProperty("case").GetString() == name);
    }

    private static (List<(uint Id, string? Name)> Items, PageCursor? Next) Decode(string command, byte[] reply)
    {
        return command switch
        {
            "get_streams" => Items(BinaryMapper.MapPage(reply, BinaryMapper.MapToStream),
                stream => (stream.Id, stream.Name)),
            "get_topics" => Items(BinaryMapper.MapPage(reply, BinaryMapper.MapToTopic),
                topic => (topic.Id, topic.Name)),
            "get_partitions" => Items(BinaryMapper.MapPage(reply, BinaryMapper.MapToPartition),
                partition => (partition.Id, null)),
            "get_consumer_groups" => Items(BinaryMapper.MapPage(reply, BinaryMapper.MapToConsumerGroup),
                group => (group.Id, group.Name)),
            "get_users" => Items(BinaryMapper.MapPage(reply, BinaryMapper.MapToUserResponse),
                user => (user.Id, user.Username)),
            _ => throw new ArgumentOutOfRangeException(nameof(command), command, "The fixture names an unknown list.")
        };
    }

    private static (List<(uint Id, string? Name)> Items, PageCursor? Next) Items<T>(Page<T> page,
        Func<T, (uint Id, string? Name)> item)
    {
        return ([.. page.Items.Select(item)], page.Next);
    }

    private static void AssertSentBack(PageCursor cursor, string cursorHex)
    {
        var query = new NamedPageQuery { Page = new PageQuery { Cursor = cursor } };

        query.Validate();

        Assert.Equal($"{DefaultQueryHex}{cursorHex.Length / 2:x2}{cursorHex}{NoNameHex}",
            Convert.ToHexString(TcpContracts.GetPage(query)).ToLowerInvariant());
    }

    private static void AssertRefused(Action validate)
    {
        var refusal = Assert.Throws<IggyInvalidStatusCodeException>(validate);
        Assert.Equal(VsrError.INVALID_COMMAND, refusal.StatusCode);
        Assert.False(refusal.FromServer);
    }

    private static TheoryData<string, string> Cases(JsonElement parent, params string[] groups)
    {
        var cases = new TheoryData<string, string>();
        foreach (var group in groups)
        {
            foreach (var vector in parent.GetProperty(group).EnumerateArray())
            {
                cases.Add(group, vector.GetProperty("case").GetString()!);
            }
        }

        return cases;
    }
}
