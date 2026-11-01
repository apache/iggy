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

using System.Collections.Concurrent;
using System.Net;
using System.Text;
using System.Web;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Extensions;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;
using Apache.Iggy.Vsr;

namespace Apache.Iggy.Tests.ClientTests;

/// <summary>
///     A poll that stores its offset changes state, so it is a POST that no client or proxy sends again. The server
///     refuses a GET that asks for auto commit.
/// </summary>
public sealed class HttpPartitionPollTests
{
    private const string DeferredPollPath = "/streams/1/topics/2/messages/deferred";

    /// <summary>A reply with one message at offset 42 under the context 17, 9, 52.</summary>
    private const string Batch = """
        {
          "partition_id": 1,
          "current_offset": 42,
          "context": { "incarnation": 17, "owner_generation": 9, "metadata_op": 52 },
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_poll_options_when_polling_should_post_only_a_poll_that_commits(bool onRead)
    {
        var server = new RecordingHandler();
        var options = new PollOptions
        {
            MaxWait = TimeSpan.FromSeconds(2),
            Count = 5,
            Commit = onRead ? CommitPolicy.OnRead : CommitPolicy.Manual
        };

        var polled = await Client(server).PollMessagesAsync(Stream, Topic, 1, Consumer.New(1), PollingStrategy.Next(),
            options, TestContext.Current.CancellationToken);

        var request = Assert.Single(server.Requests);
        Assert.Equal((onRead ? HttpMethod.Post : HttpMethod.Get, DeferredPollPath), (request.Method, request.Path));
        Assert.Equal(onRead ? "true" : "false", request.Query["auto_commit"]);
        Assert.Equal("5", request.Query["count"]);
        Assert.Equal(PollingStrategy.Continuation(1, MessagePolling.Offset, 43, new PartitionContext(17, 9, 52)),
            polled.Continuation);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_plain_poll_when_it_commits_should_post_a_deferred_poll_without_a_wait(bool autoCommit)
    {
        var server = new RecordingHandler();

        await Client(server).PollMessagesAsync(Stream, Topic, 1, Consumer.New(1), PollingStrategy.Next(), 10,
            autoCommit, TestContext.Current.CancellationToken);

        var request = Assert.Single(server.Requests);
        if (autoCommit)
        {
            Assert.Equal((HttpMethod.Post, DeferredPollPath), (request.Method, request.Path));
            Assert.Equal("0", request.Query["wait_us"]);
        }
        else
        {
            Assert.Equal((HttpMethod.Get, "/streams/1/topics/2/messages"), (request.Method, request.Path));
        }
    }

    [Fact]
    public async Task given_auto_commit_when_polling_should_refuse_it_before_sending()
    {
        var server = new RecordingHandler();

        var refused = await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => Client(server).PollMessagesAsync(
            Stream, Topic, 1, Consumer.New(1), PollingStrategy.Next(), new PollOptions { Commit = CommitPolicy.Auto() },
            TestContext.Current.CancellationToken));

        Assert.Equal(VsrError.INVALID_CONFIGURATION, refused.StatusCode);
        Assert.Empty(server.Requests);
    }

    [Fact]
    public async Task given_a_standalone_reader_over_http_when_reading_should_poll_each_partition_on_its_own()
    {
        var server = new RecordingHandler();
        await using var reader = await Client(server).Reader(Stream, Topic, Consumer.New(1))
            .WithPartitions(1, 2)
            .WithOptions(new PollOptions { MaxWait = TimeSpan.FromMilliseconds(10) })
            .BuildAsync(TestContext.Current.CancellationToken);

        var seen = new HashSet<uint>();
        while (seen.Count < 2)
        {
            seen.Add((await reader.NextAsync(TestContext.Current.CancellationToken)).PartitionId);
        }

        Assert.Equal([1u, 2u], seen.Order());
        Assert.All(server.Requests, request =>
            Assert.Equal((HttpMethod.Get, DeferredPollPath), (request.Method, request.Path)));
        Assert.Equal(["1", "2"], server.Requests.Select(request => request.Query["partition_id"]).Distinct().Order());
    }

    private static HttpMessageStream Client(HttpMessageHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }

    private sealed record RecordedRequest(HttpMethod Method, string Path, System.Collections.Specialized.NameValueCollection Query);

    /// <summary>Answers every poll with <see cref="Batch" /> and records its method, path and query.</summary>
    private sealed class RecordingHandler : HttpMessageHandler
    {
        internal ConcurrentQueue<RecordedRequest> Requests { get; } = new();

        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            var query = HttpUtility.ParseQueryString(request.RequestUri!.Query);
            Requests.Enqueue(new RecordedRequest(request.Method, request.RequestUri.AbsolutePath, query));
            var partition = query["partition_id"] ?? "1";
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = request,
                Content = new StringContent(Batch.Replace("\"partition_id\": 1", $"\"partition_id\": {partition}"),
                    Encoding.UTF8, "application/json")
            });
        }
    }
}
