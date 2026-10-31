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
using System.Text;
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Tests.ClientTests;

/// <summary>
///     The server stores the offset of a poll with auto commit before it answers, so a 504 can hide a batch it
///     already moved past. Such a poll is never sent again and its outcome is reported as unknown. A poll without
///     auto commit moves nothing and is sent again.
/// </summary>
public sealed class HttpCommittingPollTests
{
    private const string PollPath = "/streams/1/topics/2/messages";

    private const string TimedOut = """{"id":57,"code":"transient_not_committed","reason":"timed out"}""";

    /// <summary>A reply with one message at offset 42.</summary>
    private const string Batch = """
        {
          "partition_id": 1,
          "current_offset": 42,
          "messages": [
            {
              "header": {
                "checksum": 0, "id": 1, "offset": 42, "timestamp": 0, "origin_timestamp": 0,
                "user_headers_length": 0, "payload_length": 5, "reserved": 0
              },
              "payload": "aGVsbG8="
            }
          ]
        }
        """;

    private static readonly Identifier Stream = Identifier.Numeric(1);
    private static readonly Identifier Topic = Identifier.Numeric(2);

    [Fact]
    public async Task given_a_committing_poll_when_the_server_times_out_should_not_send_it_again_and_report_an_unknown_outcome()
    {
        var server = new ScriptedHandler((HttpStatusCode.GatewayTimeout, TimedOut), (HttpStatusCode.OK, Batch));

        var error = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() =>
            PollAsync(Client(server), true));

        Assert.Equal(57, Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode);
        Assert.NotNull(error.OperationId);
        Assert.Equal(1, server.Requests);
    }

    /// <summary>
    ///     A committing poll whose outcome is unknown may still have stored its offset. Its partition takes no
    ///     committing poll and no offset store of the same topic until the caller abandons it, while the same partition
    ///     id of another topic goes on.
    /// </summary>
    [Fact]
    public async Task given_a_committing_poll_with_an_unknown_outcome_when_using_its_partition_should_block_it_until_abandoned()
    {
        var server = new RoutedHandler(TimedOut);
        var client = Client(server);
        IIggyClient recovery = client;
        var token = TestContext.Current.CancellationToken;

        var uncertain = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => PollAsync(client, true));
        var poll = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => PollAsync(client, true));
        var store = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() =>
            client.StoreOffsetAsync(Consumer.New(1), Stream, Topic, 41, 1, token));
        var otherTopic = await client.PollMessagesAsync(Stream, Identifier.Numeric(3), 1, Consumer.New(1),
            PollingStrategy.Next(), 10, true, token);
        await recovery.AbandonOperationAsync(uncertain.OperationId!.Value);
        var polled = await PollAsync(client, true);
        await client.StoreOffsetAsync(Consumer.New(1), Stream, Topic, 42, 1, token);

        Assert.Equal((uncertain.OperationId.Value, 1u), (poll.OperationId, poll.PartitionId));
        Assert.Equal((uncertain.OperationId.Value, 1u), (store.OperationId, store.PartitionId));
        Assert.Single(otherTopic.Messages);
        Assert.Single(polled.Messages);
        Assert.Equal(3, server.Polls);
        Assert.Equal(1, server.Stores);
        await Assert.ThrowsAsync<VsrOperationNotRetainedException>(() =>
            recovery.AbandonOperationAsync(uncertain.OperationId.Value));
    }

    [Fact]
    public async Task given_a_poll_without_auto_commit_when_the_server_times_out_should_send_it_again()
    {
        var server = new ScriptedHandler((HttpStatusCode.GatewayTimeout, TimedOut), (HttpStatusCode.OK, Batch));

        var polled = await PollAsync(Client(server), false);

        Assert.Equal(42ul, Assert.Single(polled.Messages).Header.Offset);
        Assert.Equal(2, server.Requests);
    }

    private static HttpMessageStream Client(HttpMessageHandler server)
    {
        return new HttpMessageStream(new HttpClient(new TransientHttpRetryHandler(server))
        {
            BaseAddress = new Uri("http://localhost")
        });
    }

    private static Task<PolledMessages> PollAsync(HttpMessageStream client, bool autoCommit)
    {
        return client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(1), PollingStrategy.Next(), 10, autoCommit,
            TestContext.Current.CancellationToken);
    }

    /// <summary>
    ///     Two committing polls of one partition run at once. The second waits for the first, whose outcome turns out
    ///     unknown, and then fails as blocked by it without going out.
    /// </summary>
    [Fact]
    public async Task given_a_committing_poll_in_flight_when_another_committing_poll_of_its_partition_starts_should_wait_and_see_its_block()
    {
        var server = new GatedHandler(TimedOut);
        var client = Client(server);
        var token = TestContext.Current.CancellationToken;
        var first = PollAsync(client, true);
        await server.Held.Task.WaitAsync(TimeSpan.FromSeconds(5), token);

        var second = PollAsync(client, true);
        // The second poll would reach the server now.
        await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        var sentWhileFirstRan = server.Polls;
        server.Release.SetResult();
        var uncertain = await Assert.ThrowsAsync<VsrFetchProgressUncertainException>(() => first);
        var blocked = await Assert.ThrowsAsync<VsrPartitionLaneBlockedException>(() => second);

        Assert.Equal(1, sentWhileFirstRan);
        Assert.Equal((uncertain.OperationId!.Value, 1u), (blocked.OperationId, blocked.PartitionId));
        Assert.Equal(1, server.Polls);
    }

    /// <summary>Holds the first poll until <see cref="Release" />, then answers it with a gateway timeout.</summary>
    private sealed class GatedHandler(string timedOut) : HttpMessageHandler
    {
        private int _polls;

        internal TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal TaskCompletionSource Release { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int Polls => Volatile.Read(ref _polls);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var first = Interlocked.Increment(ref _polls) == 1;
            if (first)
            {
                Held.SetResult();
                await Release.Task.WaitAsync(ct);
            }

            var (status, body) = first ? (HttpStatusCode.GatewayTimeout, timedOut) : (HttpStatusCode.OK, Batch);
            return new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            };
        }
    }

    /// <summary>
    ///     Answers the first poll of topic 2 with a gateway timeout and every other poll with the batch, and stores
    ///     each offset.
    /// </summary>
    private sealed class RoutedHandler(string timedOut) : HttpMessageHandler
    {
        private int _polls;
        private int _stores;

        internal int Polls => Volatile.Read(ref _polls);

        internal int Stores => Volatile.Read(ref _stores);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (request.RequestUri!.AbsolutePath.EndsWith("/consumer-offsets"))
            {
                Interlocked.Increment(ref _stores);
                return Task.FromResult(new HttpResponseMessage(HttpStatusCode.NoContent) { RequestMessage = request });
            }

            var first = Interlocked.Increment(ref _polls) == 1;
            var (status, body) = first ? (HttpStatusCode.GatewayTimeout, timedOut) : (HttpStatusCode.OK, Batch);
            return Task.FromResult(new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });
        }
    }

    /// <summary>Answers each poll with the next scripted status and body.</summary>
    private sealed class ScriptedHandler(params (HttpStatusCode Status, string Body)[] replies) : HttpMessageHandler
    {
        private int _requests;

        internal int Requests => Volatile.Read(ref _requests);

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Assert.StartsWith(PollPath, request.RequestUri!.AbsolutePath);
            var (status, body) = replies[Interlocked.Increment(ref _requests) - 1];
            return Task.FromResult(new HttpResponseMessage(status)
            {
                RequestMessage = request,
                Content = new StringContent(body, Encoding.UTF8, "application/json")
            });
        }
    }
}
