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
using Apache.Iggy.Contracts;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Tests.ClientTests;

public sealed class HttpConsumerOffsetTests
{
    private static readonly TimeSpan ClientTimeout = TimeSpan.FromMilliseconds(50);

    [Fact]
    public async Task given_a_polled_context_when_storing_an_offset_should_send_it()
    {
        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        await client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), 100, 3,
            new PartitionContext(17, 9, 52), TestContext.Current.CancellationToken);

        Assert.Contains("\"context\":{\"incarnation\":17,\"owner_generation\":9,\"metadata_op\":52}",
            handler.RequestBody);
    }

    [Fact]
    public async Task given_a_fully_zero_context_when_storing_an_offset_should_send_none()
    {
        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        await client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), 100, 3,
            default(PartitionContext), TestContext.Current.CancellationToken);

        Assert.Contains("\"context\":null", handler.RequestBody);
    }

    /// <summary>
    ///     HTTP has no route to take an incarnation from, and a drop would remove the owner fence, so the server gets
    ///     the context and refuses it with status 87.
    /// </summary>
    [Fact]
    public async Task given_a_context_of_incarnation_0_with_an_owner_when_storing_an_offset_should_send_it_as_it_is()
    {
        var handler = new HttpTopicOptionsTests.StubHandler(string.Empty);
        var client = new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });

        await client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), 100, 3,
            new PartitionContext(0, 9, 100), TestContext.Current.CancellationToken);

        Assert.Contains("\"context\":{\"incarnation\":0,\"owner_generation\":9,\"metadata_op\":100}",
            handler.RequestBody);
    }

    /// <summary>The server may have applied a store or a delete before it answered with a server error.</summary>
    [Theory]
    [InlineData(HttpStatusCode.GatewayTimeout, false)]
    [InlineData(HttpStatusCode.GatewayTimeout, true)]
    [InlineData(HttpStatusCode.InternalServerError, false)]
    [InlineData(HttpStatusCode.InternalServerError, true)]
    public async Task given_a_server_error_when_writing_an_offset_should_report_an_unknown_outcome(
        HttpStatusCode status, bool deletes)
    {
        var client = Client(new HttpTopicOptionsTests.StubHandler(string.Empty) { StatusCode = status });

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => WriteAsync(client, deletes));
    }

    /// <summary>A 503 refuses a store or a delete before the server takes it.</summary>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task given_a_refusal_before_the_write_when_writing_an_offset_should_report_the_refusal(bool deletes)
    {
        var client = Client(new HttpTopicOptionsTests.StubHandler(string.Empty)
        {
            StatusCode = HttpStatusCode.ServiceUnavailable
        });

        await Assert.ThrowsAsync<IggyInvalidStatusCodeException>(() => WriteAsync(client, deletes));
    }

    /// <summary>The server may have applied a store or a delete before the connection closed or the client gave up.</summary>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task given_a_lost_reply_when_writing_an_offset_should_report_an_unknown_outcome(bool timesOut,
        bool deletes)
    {
        var client = Client(new LostReplyHandler(timesOut));

        await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => WriteAsync(client, deletes));
    }

    private static HttpMessageStream Client(HttpMessageHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler)
        {
            BaseAddress = new Uri("http://localhost"),
            Timeout = ClientTimeout
        });
    }

    private static Task WriteAsync(HttpMessageStream client, bool deletes)
    {
        return deletes
            ? client.DeleteOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), 3,
                TestContext.Current.CancellationToken)
            : client.StoreOffsetAsync(Consumer.New(1), Identifier.Numeric(1), Identifier.Numeric(2), 100, 3,
                new PartitionContext(17, 9, 52), TestContext.Current.CancellationToken);
    }

    /// <summary>Closes the connection after the send, or answers only after the client gave up.</summary>
    private sealed class LostReplyHandler(bool timesOut) : HttpMessageHandler
    {
        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            if (timesOut)
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            }

            throw new HttpRequestException(HttpRequestError.ResponseEnded, "The connection closed before the reply.");
        }
    }
}
