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

using System.Buffers.Binary;
using System.Text;
using System.Text.Json;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Tcp;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Extensions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Mappers;
using Apache.Iggy.Tests.ConsumerTests;
using Apache.Iggy.Tests.VsrTests;
using Apache.Iggy.Utils;
using Apache.Iggy.Vsr;
using Microsoft.Extensions.Logging.Abstractions;
using static Apache.Iggy.Tests.VsrTests.MockFrames;

namespace Apache.Iggy.Tests.ContractsTests;

/// <summary>
///     The deferred polling wire vectors that the Rust encoder generates for every SDK: each request encodes from its
///     fields to the same bytes, each reply decodes to the same sections and messages, and each refused vector is
///     refused.
/// </summary>
public sealed class DeferredPollingFixtureTests
{
    private const int OffsetWireKind = 1;

    private static readonly JsonElement Fixture = JsonDocument
        .Parse(File.ReadAllText(Path.Combine(AppContext.BaseDirectory, "Fixtures", "deferred_polling.json")))
        .RootElement;

    public static TheoryData<string> DeferredPolls => Cases("deferred_polls");

    public static TheoryData<string> Fetches => Cases("fetches");

    public static TheoryData<string> Cancellations => Cases("cancellations");

    public static TheoryData<string> FetchReplies => Cases("fetch_replies");

    public static TheoryData<string> RefusedFetchReplies => Cases("refused", "fetch_replies");

    public static TheoryData<string> RefusedFetches => Cases("refused", "fetches");

    [Fact]
    public void given_the_fixture_constants_when_compared_should_match_the_client()
    {
        var constants = Fixture.GetProperty("constants");
        var codes = constants.GetProperty("codes");
        var errors = constants.GetProperty("errors");

        Assert.Equal(
            (CommandCodes.POLL_MESSAGES_CODE, CommandCodes.POLL_MESSAGES_DEFERRED_CODE,
                CommandCodes.CANCEL_DEFERRED_REQUEST_CODE, CommandCodes.FETCH_MESSAGES_CODE),
            (codes.GetProperty("poll_messages").GetInt32(), codes.GetProperty("poll_messages_deferred").GetInt32(),
                codes.GetProperty("cancel_deferred_request").GetInt32(), codes.GetProperty("fetch_messages").GetInt32()));
        Assert.Equal(
            (VsrError.TRANSIENT_NOT_COMMITTED, VsrError.TRANSIENT_NOT_ACCEPTED, VsrError.INVALID_SIZE_BYTES,
                VsrError.HISTORY_UNAVAILABLE, VsrError.NOT_PRIMARY, VsrError.DEFERRED_POLL_CAPACITY_EXCEEDED,
                VsrError.PARTITION_LANE_BLOCKED, VsrError.FETCH_PROGRESS_UNCERTAIN),
            (errors.GetProperty("transient_not_committed").GetInt32(),
                errors.GetProperty("transient_not_accepted").GetInt32(),
                errors.GetProperty("invalid_size_bytes").GetInt32(), errors.GetProperty("history_unavailable").GetInt32(),
                errors.GetProperty("not_primary").GetInt32(),
                errors.GetProperty("deferred_poll_capacity_exceeded").GetInt32(),
                errors.GetProperty("partition_lane_blocked").GetInt32(),
                errors.GetProperty("fetch_progress_uncertain").GetInt32()));
        Assert.Equal(
            (DeferredPollOptions.MIN_MAX_BYTES, TcpContracts.MAX_PARTITIONS_PER_REQUEST, TcpContracts.MAX_RECORD_SIZE,
                TcpContracts.MAX_RECORD_REPLY_SIZE, VsrHeader.HEADER_SIZE, FetchedMessages.SECTION_HEADER_SIZE),
            (constants.GetProperty("min_deferred_poll_max_bytes").GetUInt32(),
                constants.GetProperty("max_partitions_per_request").GetInt32(),
                constants.GetProperty("max_record_size").GetInt32(),
                constants.GetProperty("max_record_reply_size").GetInt32(),
                constants.GetProperty("request_header_size").GetInt32(),
                constants.GetProperty("fetch_section_header_size").GetInt32()));
    }

    [Theory]
    [MemberData(nameof(DeferredPolls))]
    public void given_a_deferred_poll_vector_when_encoded_should_match_its_body(string name)
    {
        var vector = Case("deferred_polls", name);
        var request = vector.GetProperty("request");
        var expected = Convert.FromHexString(vector.GetProperty("body_hex").GetString()!);
        var body = new byte[expected.Length];

        TcpContracts.GetMessages(body, ConsumerOf(request.GetProperty("consumer")),
            IdentifierOf(request.GetProperty("stream_id")), IdentifierOf(request.GetProperty("topic_id")),
            StrategyOf(request.GetProperty("strategy")), request.GetProperty("count").GetUInt32(),
            request.GetProperty("auto_commit").GetBoolean(), request.GetProperty("partition_id").GetUInt32(),
            OptionsOf(request));

        Assert.Equal(Convert.ToHexString(expected), Convert.ToHexString(body));
    }

    [Theory]
    [MemberData(nameof(Fetches))]
    public void given_a_fetch_vector_when_encoded_should_match_its_body(string name)
    {
        var vector = Case("fetches", name);
        var request = vector.GetProperty("request");
        var targets = request.GetProperty("targets").EnumerateArray().ToList();

        var body = TcpContracts.FetchMessages(ConsumerOf(request.GetProperty("consumer")),
            IdentifierOf(request.GetProperty("stream_id")), IdentifierOf(request.GetProperty("topic_id")),
            OptionsOf(request), request.GetProperty("count").GetUInt32(), request.GetProperty("auto_commit").GetBoolean(),
            targets[0].GetProperty("max_bytes").GetUInt32(),
            [
                .. targets.Select(target => (target.GetProperty("partition_id").GetUInt32(),
                    StrategyOf(target.GetProperty("strategy")).WithContext(new PartitionContext(
                        target.GetProperty("incarnation").GetUInt64(),
                        target.GetProperty("owner_generation").GetUInt64(), 0))))
            ]);

        Assert.All(targets, target => Assert.Equal(targets[0].GetProperty("max_bytes").GetUInt32(),
            target.GetProperty("max_bytes").GetUInt32()));
        Assert.Equal(vector.GetProperty("body_hex").GetString(), Convert.ToHexString(body).ToLowerInvariant());
    }

    [Theory]
    [MemberData(nameof(Cancellations))]
    public void given_a_cancellation_vector_when_encoded_should_match_its_header(string name)
    {
        var vector = Case("cancellations", name);
        var header = new byte[VsrHeader.HEADER_SIZE];

        VsrHeader.EncodeRequestHeader(header, new ConsensusSession(), CommandCodes.CANCEL_DEFERRED_REQUEST_CODE,
            ReadOnlySpan<byte>.Empty, retainedFrame: new SessionFrame(vector.GetProperty("client").GetUInt64(),
                vector.GetProperty("request").GetUInt64(), vector.GetProperty("session").GetUInt64()));

        Assert.Equal(vector.GetProperty("header_hex").GetString(), Convert.ToHexString(header).ToLowerInvariant());
    }

    [Theory]
    [MemberData(nameof(FetchReplies))]
    public void given_a_fetch_reply_vector_when_decoded_should_hold_its_sections_and_messages(string name)
    {
        var vector = Case("fetch_replies", name);
        using var reply = Decode(vector.GetProperty("body_hex").GetString()!);
        var expected = vector.GetProperty("sections").EnumerateArray().ToList();

        Assert.Equal(expected.Count, reply.Sections.Count);
        foreach (var (section, wanted) in reply.Sections.Zip(expected))
        {
            var context = wanted.GetProperty("context");
            Assert.Equal(
                (wanted.GetProperty("partition_id").GetUInt32(), wanted.GetProperty("status").GetUInt32(),
                    wanted.GetProperty("current_offset").GetUInt64(), wanted.GetProperty("messages_count").GetUInt32(),
                    new PartitionContext(context.GetProperty("incarnation").GetUInt64(),
                        context.GetProperty("owner_generation").GetUInt64(),
                        context.GetProperty("metadata_op").GetUInt64()),
                    wanted.GetProperty("next_offset").GetUInt64(), wanted.GetProperty("records_len").GetInt32()),
                (section.PartitionId, section.Status, section.CurrentOffset, section.MessagesCount, section.Context,
                    section.NextOffset, section.Records.Length));
            var messages = section.Status == 0 ? section.Decode().Messages : [];
            Assert.Equal(
                wanted.GetProperty("messages").EnumerateArray().Select(message => (
                    message.GetProperty("offset").GetUInt64(), UInt128.Parse(message.GetProperty("id").GetString()!),
                    message.GetProperty("timestamp").GetUInt64(), message.GetProperty("origin_timestamp").GetUInt64(),
                    message.GetProperty("payload").GetString())),
                messages.Select(message => (message.Header.Offset, message.Header.Id,
                    DateTimeOffsetUtils.ToUnixTimeMicroSeconds(message.Header.Timestamp), message.Header.OriginTimestamp,
                    (string?)Encoding.UTF8.GetString(message.Payload))));
        }
    }

    [Theory]
    [MemberData(nameof(RefusedFetchReplies))]
    public void given_a_refused_fetch_reply_vector_when_decoded_should_refuse_it(string name)
    {
        var vector = Case(Fixture.GetProperty("refused"), "fetch_replies", name);

        Assert.Throws<MalformedResponseException>(() => Decode(vector.GetProperty("body_hex").GetString()!).Dispose());
    }

    /// <summary>
    ///     The client validates a fetch before it encodes it, so each refused fetch is decoded into the arguments of
    ///     the call and refused before anything reaches the node. The client sends one byte limit for every target,
    ///     so a target limit above the request limit cannot be expressed and is refused by construction.
    /// </summary>
    [Theory]
    [MemberData(nameof(RefusedFetches))]
    public async Task given_a_refused_fetch_vector_when_fetched_should_refuse_it_before_sending(string name)
    {
        var body = Convert.FromHexString(Case(Fixture.GetProperty("refused"), "fetches", name)
            .GetProperty("body_hex").GetString()!);
        var request = DecodeFetch(body);
        if (request.Targets.Any(target => target.MaxBytes > request.MaxBytes))
        {
            return;
        }

        using var node = new MockNode();
        node.Serve(served => served.Code == GET_CLUSTER_METADATA_CODE ? StandaloneRoster(node.Port) : Answer(served));
        using var client = new TcpMessageStream(new IggyClientConfigurator
        {
            BaseAddress = $"127.0.0.1:{node.Port}",
            Protocol = Protocol.Tcp
        }, NullLoggerFactory.Instance);
        await client.ConnectAsync(TestContext.Current.CancellationToken);
        await client.LoginUserAsync("user", "secret", TestContext.Current.CancellationToken);

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => client.FetchMessagesAsync(
            Identifier.Numeric(1), Identifier.Numeric(2),
            [.. request.Targets.Select(target => (target.PartitionId, new PollingStrategy
            {
                Kind = target.Kind,
                Value = target.Value
            }))],
            Consumer.New(1), request.Count, request.AutoCommit, new DeferredPollOptions
            {
                MaxWait = TimeSpan.FromMicroseconds(request.WaitUs),
                MinCount = request.MinCount,
                MaxBytes = request.MaxBytes,
                RequestTimeout = TimeSpan.FromMicroseconds(request.RequestTimeoutUs)
            }, TestContext.Current.CancellationToken));
        Assert.Equal(0, node.Requests(CommandCodes.FETCH_MESSAGES_CODE));
    }

    private static TheoryData<string> Cases(params string[] path)
    {
        var element = path.Aggregate(Fixture, (current, key) => current.GetProperty(key));
        return [.. element.EnumerateArray().Select(vector => vector.GetProperty("case").GetString()!)];
    }

    private static JsonElement Case(string section, string name)
    {
        return Case(Fixture, section, name);
    }

    private static JsonElement Case(JsonElement parent, string section, string name)
    {
        return parent.GetProperty(section).EnumerateArray()
            .Single(vector => vector.GetProperty("case").GetString() == name);
    }

    private static FetchedMessages Decode(string bodyHex)
    {
        var body = Convert.FromHexString(bodyHex);
        var owner = new RentedBatchTests.TrackingMemoryOwner(body.Length);
        body.CopyTo(owner.Memory);
        return BinaryMapper.MapFetchedMessages(owner, null);
    }

    /// <summary>A refused fetch body: the consumer, stream and topic of the vectors, then the fields and the targets.</summary>
    private static FetchRequest DecodeFetch(byte[] body)
    {
        var request = new MockRequest(0, CommandCodes.FETCH_MESSAGES_CODE, 0) { Body = body };
        return ReadFetch(request);
    }

    private static Consumer ConsumerOf(JsonElement consumer)
    {
        var id = IdentifierOf(consumer.GetProperty("id"));
        return consumer.GetProperty("kind").GetInt32() == 1
            ? new Consumer { Type = ConsumerType.Consumer, ConsumerId = id }
            : new Consumer { Type = ConsumerType.ConsumerGroup, ConsumerId = id };
    }

    private static Identifier IdentifierOf(JsonElement identifier)
    {
        return identifier.GetProperty("kind").GetString() == "numeric"
            ? Identifier.Numeric(identifier.GetProperty("value").GetUInt32())
            : Identifier.String(identifier.GetProperty("value").GetString()!);
    }

    private static PollingStrategy StrategyOf(JsonElement strategy)
    {
        return new PollingStrategy
        {
            Kind = (MessagePolling)(strategy.GetProperty("kind").GetInt32() - OffsetWireKind),
            Value = strategy.GetProperty("value").GetUInt64()
        };
    }

    private static DeferredPollOptions OptionsOf(JsonElement request)
    {
        return new DeferredPollOptions
        {
            MaxWait = TimeSpan.FromMicroseconds(request.GetProperty("wait_us").GetUInt64()),
            MinCount = request.GetProperty("min_count").GetUInt32(),
            MaxBytes = request.GetProperty("max_bytes").GetUInt32(),
            RequestTimeout = TimeSpan.FromMicroseconds(request.GetProperty("request_timeout_us").GetUInt64())
        };
    }
}
