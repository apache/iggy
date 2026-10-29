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
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient.Implementations;
using Apache.Iggy.Kinds;

namespace Apache.Iggy.Tests.ClientTests;

/// <summary>
///     A deferred poll over HTTP that the caller cancels while the server holds it, as the TCP client reports it: a
///     poll that can move an offset has an unknown outcome, because the server can move the offset before it sees
///     that the client gave up. Any other canceled poll ends as canceled. A cancel after the reply arrived does not
///     hide the batch.
/// </summary>
public sealed class HttpDeferredPollTests
{
    private const string DeferredPollPath = "/streams/1/topics/2/messages/deferred";

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

    /// <summary>Longer than any test, so a poll the handler holds stays held until the test cancels it.</summary>
    private static readonly DeferredPollOptions LongWait = new()
    {
        MaxWait = TimeSpan.FromSeconds(30),
        RequestTimeout = TimeSpan.FromSeconds(31)
    };

    /// <summary>How long a negative check waits for a request that must not come.</summary>
    private static readonly TimeSpan SettleDelay = TimeSpan.FromMilliseconds(200);

    [Theory]
    [InlineData(MessagePolling.Next, false)]
    [InlineData(MessagePolling.Offset, true)]
    public async Task given_a_held_progress_poll_when_the_caller_cancels_should_report_an_unknown_outcome(
        MessagePolling kind, bool autoCommit)
    {
        var handler = new HoldingHandler();
        var client = Client(handler);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var poll = PollAsync(client, kind, autoCommit, cancellation.Token);
        await handler.Held.Task.WaitAsync(TestContext.Current.CancellationToken);

        await cancellation.CancelAsync();

        var error = await Assert.ThrowsAsync<VsrRequestOutcomeUnknownException>(() => poll);
        Assert.IsAssignableFrom<OperationCanceledException>(error.InnerException);
    }

    [Fact]
    public async Task given_a_held_poll_that_moves_no_offset_when_the_caller_cancels_should_end_as_canceled()
    {
        var handler = new HoldingHandler();
        var client = Client(handler);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var poll = PollAsync(client, MessagePolling.Offset, false, cancellation.Token);
        await handler.Held.Task.WaitAsync(TestContext.Current.CancellationToken);

        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
    }

    /// <summary>
    ///     A poll that waits for a free parked slot was never sent, so even one that can move an offset ends as
    ///     canceled.
    /// </summary>
    [Fact]
    public async Task given_a_progress_poll_waiting_for_a_parked_slot_when_the_caller_cancels_should_end_as_canceled_unsent()
    {
        const int Limit = DeferredPollOptions.MAX_PARKED_REQUESTS;
        var handler = new HoldingHandler();
        var client = Client(handler);
        using var held = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        using var waiting = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var polls = Enumerable.Range(0, Limit)
            .Select(_ => PollAsync(client, MessagePolling.Offset, false, held.Token)).ToArray();
        await UntilAsync(() => handler.Polls == Limit);
        var queued = PollAsync(client, MessagePolling.Next, false, waiting.Token);
        await Task.Delay(SettleDelay, TestContext.Current.CancellationToken);

        await waiting.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => queued);
        Assert.Equal(Limit, handler.Polls);
        await held.CancelAsync();
        foreach (var poll in polls)
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => poll);
        }
    }

    /// <summary>
    ///     The caller cancels after the reply of a progress poll arrived, before the client decodes it. The server
    ///     already moved the offset, so the client returns the batch or reports an unknown outcome, never a plain
    ///     cancel that hides the batch.
    /// </summary>
    [Fact]
    public async Task given_the_reply_of_a_progress_poll_when_the_caller_cancels_before_the_decode_should_not_hide_the_batch()
    {
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        var client = Client(new ReplyHandler(new CancelingContent(Batch, cancellation)));

        var poll = PollAsync(client, MessagePolling.Next, false, cancellation.Token);
        var error = await Record.ExceptionAsync(() => poll);

        Assert.True(cancellation.IsCancellationRequested);
        if (error is null)
        {
            Assert.Equal(42ul, Assert.Single((await poll).Messages).Header.Offset);
        }
        else
        {
            Assert.IsType<VsrRequestOutcomeUnknownException>(error);
        }
    }

    private static HttpMessageStream Client(HttpMessageHandler handler)
    {
        return new HttpMessageStream(new HttpClient(handler) { BaseAddress = new Uri("http://localhost") });
    }

    private static Task<PolledMessages> PollAsync(HttpMessageStream client, MessagePolling kind, bool autoCommit,
        CancellationToken token)
    {
        return client.PollMessagesAsync(Stream, Topic, 1, Consumer.New(1),
            new PollingStrategy { Kind = kind, Value = 0 }, 10, autoCommit, LongWait, token);
    }

    private static async Task UntilAsync(Func<bool> predicate)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(TestContext.Current.CancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(10));
        while (!predicate())
        {
            await Task.Delay(5, timeout.Token);
        }
    }

    /// <summary>Holds every deferred poll until its request is canceled, as a server holds a parked poll.</summary>
    private sealed class HoldingHandler : HttpMessageHandler
    {
        private int _polls;

        internal TaskCompletionSource Held { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal int Polls => Volatile.Read(ref _polls);

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request,
            CancellationToken ct)
        {
            Assert.Equal(DeferredPollPath, request.RequestUri!.AbsolutePath);
            Interlocked.Increment(ref _polls);
            Held.TrySetResult();
            await Task.Delay(Timeout.Infinite, ct);
            throw new OperationCanceledException(ct);
        }
    }

    /// <summary>Answers every deferred poll at once with <paramref name="content" />.</summary>
    private sealed class ReplyHandler(HttpContent content) : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken ct)
        {
            Assert.Equal(DeferredPollPath, request.RequestUri!.AbsolutePath);
            return Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)
            {
                RequestMessage = request,
                Content = content
            });
        }
    }

    /// <summary>
    ///     A reply body that cancels the caller once it is written. HttpClient reads the whole body before SendAsync
    ///     returns, so the cancel lands after the reply arrived and before the client decodes it.
    /// </summary>
    private sealed class CancelingContent : HttpContent
    {
        private readonly byte[] _body;
        private readonly CancellationTokenSource _caller;

        internal CancelingContent(string json, CancellationTokenSource caller)
        {
            _body = Encoding.UTF8.GetBytes(json);
            _caller = caller;
            Headers.ContentType = new MediaTypeHeaderValue("application/json") { CharSet = "utf-8" };
        }

        protected override async Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            await stream.WriteAsync(_body);
            await _caller.CancelAsync();
        }

        protected override bool TryComputeLength(out long length)
        {
            length = _body.Length;
            return true;
        }
    }
}
