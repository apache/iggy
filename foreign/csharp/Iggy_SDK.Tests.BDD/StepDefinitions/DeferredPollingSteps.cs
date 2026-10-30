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

using System.Diagnostics;
using System.Text;
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.Extensions;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging.Abstractions;
using Reqnroll;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;
using TestContext = Apache.Iggy.Tests.BDD.Context.TestContext;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

/// <summary>
///     A start step leaves its deferred poll, reader or consumer waiting on the server, and a later step sends the
///     message that wakes it. All of them share the client of the scenario.
/// </summary>
[Binding]
public sealed class DeferredPollingSteps : IAsyncDisposable
{
    private const uint MessagesCount = 10;

    /// <summary>
    ///     A message that reaches the server before a poll answers it at once, so a start step waits until its poll
    ///     parks.
    /// </summary>
    private static readonly TimeSpan ParkTime = TimeSpan.FromMilliseconds(100);

    private static readonly Consumer DefaultConsumer = Consumer.New(0);

    private readonly CancellationTokenSource _cancellation = new();
    private readonly TestContext _context;
    private IggyConsumer? _consumer;
    private IAsyncEnumerator<ReceivedMessage>? _messages;
    private Task<PolledMessages>? _poll;
    private Stopwatch? _pollStarted;
    private PartitionReader? _reader;
    private StreamResponse? _stream;
    private TopicResponse? _topic;

    public DeferredPollingSteps(TestContext context)
    {
        _context = context;
    }

    public async ValueTask DisposeAsync()
    {
        await CloseConsumerAsync();
        if (_reader is not null)
        {
            await _reader.DisposeAsync();
        }

        _cancellation.Dispose();
    }

    // Other features expect a server without streams.
    [AfterScenario(Order = 0)]
    public async Task DeleteCreatedStream()
    {
        await CloseConsumerAsync();
        if (_reader is not null)
        {
            await _reader.DisposeAsync();
            _reader = null;
        }

        if (_stream is not null)
        {
            await _context.IggyClient.DeleteStreamAsync(StreamId());
        }
    }

    [Given("a stream named {string} with a topic named {string} with {int} partitions exists")]
    public async Task CreateStreamWithTopic(string streamName, string topicName, int partitions)
    {
        if (await _context.IggyClient.GetStreamByIdAsync(Identifier.String(streamName)) is not null)
        {
            await _context.IggyClient.DeleteStreamAsync(Identifier.String(streamName));
        }

        _stream = await _context.IggyClient.CreateStreamAsync(streamName);
        _topic = await _context.IggyClient.CreateTopicAsync(StreamId(), topicName, (uint)partitions);
    }

    [When("I start a deferred poll of partition {int} from offset {int} with a max wait of {int} second(s)")]
    public Task StartDeferredPoll(int partition, int offset, int maxWaitSeconds)
    {
        return StartDeferredPollAsync(partition, DefaultConsumer, PollingStrategy.Offset((ulong)offset),
            Options(maxWaitSeconds));
    }

    [When("I start a deferred poll of partition {int} for consumer {string} with the on read commit policy, a min count of {int} and a max wait of {int} second(s)")]
    public Task StartCommittingDeferredPoll(int partition, string consumerName, int minCount, int maxWaitSeconds)
    {
        return StartDeferredPollAsync(partition, Consumer.New(consumerName), PollingStrategy.Next(),
            Options(maxWaitSeconds) with { MinCount = (uint)minCount, Commit = CommitPolicy.OnRead });
    }

    [When("I send a message with payload {string} to partition {int}")]
    public async Task SendToPartition(string payload, int partition)
    {
        await _context.IggyClient.SendMessagesAsync(StreamId(), TopicId(), Partitioning.PartitionId(partition),
            [new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(payload))]);
    }

    [Then("the deferred poll should return the payload {string} within {int} second(s)")]
    public async Task DeferredPollReturns(string payload, int withinSeconds)
    {
        var polled = await Within(_poll.ShouldNotBeNull(), TimeSpan.FromSeconds(withinSeconds), "deferred poll");
        ShouldHaveSinglePayload(polled, payload);
    }

    [Then("the deferred poll should return no messages between {int} and {int} second(s) after it started")]
    public async Task DeferredPollReturnsNothing(int fromSeconds, int toSeconds)
    {
        var started = _pollStarted.ShouldNotBeNull();
        var polled = await Within(_poll.ShouldNotBeNull(), TimeSpan.FromSeconds(toSeconds) - started.Elapsed,
            "deferred poll");

        started.Elapsed.ShouldBeGreaterThanOrEqualTo(TimeSpan.FromSeconds(fromSeconds));
        polled.Messages.ShouldBeEmpty();
    }

    [Then("a poll of partition {int} for consumer {string} with the next strategy and the on read commit policy should return the payload {string}")]
    public async Task OnReadPollReturns(int partition, string consumerName, string payload)
    {
        var polled = await _context.IggyClient.PollMessagesAsync(StreamId(), TopicId(), (uint)partition,
            Consumer.New(consumerName), PollingStrategy.Next(),
            new PollOptions { Count = 1, Commit = CommitPolicy.OnRead });
        ShouldHaveSinglePayload(polled, payload);
    }

    [Then("a poll of partition {int} for consumer {string} with the next strategy should return the payload {string}")]
    public async Task NextPollReturns(int partition, string consumerName, string payload)
    {
        var polled = await _context.IggyClient.PollMessagesAsync(StreamId(), TopicId(), (uint)partition,
            Consumer.New(consumerName), PollingStrategy.Next(), new PollOptions { Count = MessagesCount });
        polled.Messages.Select(message => Encoding.UTF8.GetString(message.Payload)).FirstOrDefault()
            .ShouldBe(payload);
    }

    [When("I cancel the deferred poll after {int} milliseconds")]
    public async Task CancelDeferredPoll(int milliseconds)
    {
        var poll = _poll.ShouldNotBeNull();
        await Task.Delay(milliseconds);
        await _cancellation.CancelAsync();

        // The server answers a cancelled poll that commits once: refused when the cancel won, so the poll ends as
        // canceled and stores nothing.
        var error = await Record.ExceptionAsync(() => poll);
        error.ShouldBeAssignableTo<OperationCanceledException>(
            $"the deferred poll must end as canceled, not with {error?.ToString() ?? "a result"}");
    }

    [When("I wait {int} second(s)")]
    public Task Wait(int seconds)
    {
        return Task.Delay(TimeSpan.FromSeconds(seconds));
    }

    [When("I open a partition reader on partitions {int}, {int} and {int} from offset {int} with a max wait of {int} second(s)")]
    public async Task OpenReader(int first, int second, int third, int offset, int maxWaitSeconds)
    {
        _reader = await _context.IggyClient.Reader(StreamId(), TopicId(), DefaultConsumer)
            .WithPartitions((uint)first, (uint)second, (uint)third)
            .WithStart(PollingStrategy.Offset((ulong)offset))
            .WithOptions(Options(maxWaitSeconds))
            .BuildAsync();
    }

    [Then("the partition reader should return the payload {string} from partition {int} within {int} second(s)")]
    public async Task ReaderReturns(string payload, int partition, int withinSeconds)
    {
        var batch = await Within(_reader.ShouldNotBeNull().NextAsync().AsTask(),
            TimeSpan.FromSeconds(withinSeconds), "partition reader");

        batch.PartitionId.ShouldBe((uint)partition);
        ShouldHaveSinglePayload(batch, payload);
    }

    [When("I open a consumer in group {string} on the topic with a max wait of {int} second(s)")]
    public Task OpenGroupConsumer(string group, int maxWaitSeconds)
    {
        return OpenConsumerAsync(group, Options(maxWaitSeconds));
    }

    [When("I open a consumer in group {string} on the topic that commits after processing")]
    public Task OpenCommittingGroupConsumer(string group)
    {
        return OpenConsumerAsync(group, new PollOptions
        {
            MaxWait = TimeSpan.FromSeconds(5),
            Commit = CommitPolicy.Auto(new AutoCommit { Interval = TimeSpan.FromSeconds(5), After = CommitAfter.Processing })
        });
    }

    [Then("the consumer should return the payload {string} within {int} second(s)")]
    public async Task ConsumerReturns(string payload, int withinSeconds)
    {
        var messages = _messages.ShouldNotBeNull();
        (await Within(messages.MoveNextAsync().AsTask(), TimeSpan.FromSeconds(withinSeconds), "consumer"))
            .ShouldBeTrue();
        Encoding.UTF8.GetString(messages.Current.Message.Payload).ShouldBe(payload);
    }

    [When("I close the consumer")]
    public Task CloseConsumer()
    {
        return CloseConsumerAsync();
    }

    private async Task OpenConsumerAsync(string group, PollOptions options)
    {
        _consumer = new IggyConsumer(_context.IggyClient, new IggyConsumerConfig
        {
            StreamId = StreamId(),
            TopicId = TopicId(),
            Consumer = Consumer.Group(group),
            PollingStrategy = PollingStrategy.Next(),
            PollOptions = options
        }, NullLoggerFactory.Instance);
        await _consumer.InitAsync();
        _messages = _consumer.ReceiveAsync().GetAsyncEnumerator();
    }

    private async Task CloseConsumerAsync()
    {
        if (_consumer is null)
        {
            return;
        }

        await _consumer.ShutdownAsync();
        if (_messages is not null)
        {
            await _messages.DisposeAsync();
        }

        _consumer = null;
        _messages = null;
    }

    private async Task StartDeferredPollAsync(int partition, Consumer consumer, PollingStrategy strategy,
        PollOptions options)
    {
        _pollStarted = Stopwatch.StartNew();
        _poll = _context.IggyClient.PollMessagesAsync(StreamId(), TopicId(), (uint)partition, consumer, strategy,
            options, _cancellation.Token);

        await Task.Delay(ParkTime);
        _poll.IsCompleted.ShouldBeFalse("the deferred poll must park");
    }

    private static PollOptions Options(int maxWaitSeconds)
    {
        return new PollOptions { Count = MessagesCount, MaxWait = TimeSpan.FromSeconds(maxWaitSeconds) };
    }

    private static async Task<T> Within<T>(Task<T> call, TimeSpan limit, string name)
    {
        if (await Task.WhenAny(call, Task.Delay(limit)) != call)
        {
            Assert.Fail($"The {name} did not return within {limit}.");
        }

        return await call;
    }

    private static void ShouldHaveSinglePayload(PolledMessages polled, string payload)
    {
        Encoding.UTF8.GetString(polled.Messages.ShouldHaveSingleItem().Payload).ShouldBe(payload);
    }

    private Identifier StreamId()
    {
        return Identifier.Numeric(_stream.ShouldNotBeNull().Id);
    }

    private Identifier TopicId()
    {
        return Identifier.Numeric(_topic.ShouldNotBeNull().Id);
    }
}
