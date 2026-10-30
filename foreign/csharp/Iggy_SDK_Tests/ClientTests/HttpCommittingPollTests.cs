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

        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() =>
            PollAsync(Client(server), true));

        Assert.Equal(57, Assert.IsType<IggyInvalidStatusCodeException>(error.InnerException).StatusCode);
        Assert.Equal(1, server.Requests);
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
