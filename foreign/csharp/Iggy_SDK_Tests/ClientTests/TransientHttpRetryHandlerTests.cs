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
using System.Net.Http.Headers;
using System.Text;
using Apache.Iggy.IggyClient.Implementations;

namespace Apache.Iggy.Tests.ClientTests;

/// <summary>
///     A primary below its free-space reserve refuses a send with 507 and stores nothing, so the handler sends it
///     again, as it does after a 503.
/// </summary>
public sealed class TransientHttpRetryHandlerTests
{
    private const string SendUri = "http://localhost/streams/1/topics/2/messages";
    private const string SendBody = """{"messages":[]}""";

    private const string FreeSpaceReserveReached =
        """{"id":4109,"code":"free_space_reserve_reached","reason":"Free space on the data disk is below [node] free_space_reserve; retry after backoff on the same replica","field":null}""";

    [Fact]
    public async Task given_a_send_refused_below_the_free_space_reserve_when_space_returns_should_send_it_again()
    {
        var server = new RecordingHandler(HttpStatusCode.InsufficientStorage, HttpStatusCode.InsufficientStorage,
            HttpStatusCode.Created);
        using var client = new HttpMessageInvoker(new TransientHttpRetryHandler(server));

        using var response = await client.SendAsync(
            new HttpRequestMessage(HttpMethod.Post, SendUri)
            {
                Content = new StringContent(SendBody, Encoding.UTF8, "application/json")
            }, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.Created, response.StatusCode);
        Assert.Equal([SendBody, SendBody, SendBody], server.Bodies);
    }

    /// <summary>Answers each request with the next scripted status and records the body it got.</summary>
    private sealed class RecordingHandler(params HttpStatusCode[] statuses) : HttpMessageHandler
    {
        internal List<string> Bodies { get; } = [];

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Bodies.Add(await request.Content!.ReadAsStringAsync(ct));
            var status = statuses[Bodies.Count - 1];
            var response = new HttpResponseMessage(status) { RequestMessage = request };
            if (status == HttpStatusCode.InsufficientStorage)
            {
                response.Content = new StringContent(FreeSpaceReserveReached, Encoding.UTF8, "application/json");
                response.Headers.RetryAfter = new RetryConditionHeaderValue(TimeSpan.Zero);
            }

            return response;
        }
    }
}
