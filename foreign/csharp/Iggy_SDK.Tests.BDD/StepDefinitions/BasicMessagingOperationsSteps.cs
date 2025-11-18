// // Licensed to the Apache Software Foundation (ASF) under one
// // or more contributor license agreements.  See the NOTICE file
// // distributed with this work for additional information
// // regarding copyright ownership.  The ASF licenses this file
// // to you under the Apache License, Version 2.0 (the
// // "License"); you may not use this file except in compliance
// // with the License.  You may obtain a copy of the License at
// //
// //   http://www.apache.org/licenses/LICENSE-2.0
// //
// // Unless required by applicable law or agreed to in writing,
// // software distributed under the License is distributed on an
// // "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// // KIND, either express or implied.  See the License for the
// // specific language governing permissions and limitations
// // under the License.

using System.Text;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Factory;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;
using Apache.Iggy.Tests.BDD.Context;
using Microsoft.Extensions.Logging.Abstractions;
using Reqnroll;
using Shouldly;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Apache.Iggy.Tests.BDD.StepDefinitions;

[Binding]
public class BasicMessagingOperationsSteps
{
    private readonly TestContext _context;

    public BasicMessagingOperationsSteps(TestContext context)
    {
        _context = context;
    }

    [Given(@"I have a running Iggy server")]
    public async Task GivenIHaveARunningIggyServer()
    {
        _context.IggyClient = IggyClientFactory.CreateClient(new IggyClientConfigurator()
        {
            BaseAddress = _context.TcpUrl,
            Protocol = Protocol.Tcp
        });

        await _context.IggyClient.ConnectAsync();
        await _context.IggyClient.PingAsync();
    }

    [Given(@"I am authenticated as the root user")]
    public async Task GivenIAmAuthenticatedAsTheRootUser()
    {
        var loginResult = await _context.IggyClient.LoginUser("iggy", "iggy");

        loginResult.ShouldNotBeNull();
        loginResult.UserId.ShouldBe(0);
    }

    [Given(@"I have no streams in the system")]
    public async Task GivenIHaveNoStreamsInTheSystem()
    {
        IReadOnlyList<StreamResponse> streams = await _context.IggyClient.GetStreamsAsync();

        streams.ShouldNotBeNull();
        streams.Count.ShouldBe(0);
    }

    [When("I create a stream with name {string}")]
    public async Task WhenICreateAStreamWithName(string streamName)
    {
        _context.CreatedStream = await _context.IggyClient.CreateStreamAsync(streamName);
    }

    [Then(@"the stream should be created successfully")]
    public void ThenTheStreamShouldBeCreatedSuccessfully()
    {
        _context.CreatedStream.ShouldNotBeNull();
        _context.CreatedStream.Name.ShouldNotBeNullOrEmpty();
    }

    [Then("the stream should have name {string}")]
    public void ThenTheStreamShouldHaveName(string expectedName)
    {
        _context.CreatedStream!.Name.ShouldBe(expectedName);
    }

    [When("I create a topic with name {string} in stream {int} with {int} partitions")]
    public async Task WhenICreateATopicWithNameInStreamWithPartitions(string topicName,
        int streamId, int partitions)
    {
        _context.CreatedTopic = await _context.IggyClient.CreateTopicAsync(Identifier.Numeric(streamId),
            name: topicName,
            partitionsCount: (uint)partitions);
    }

    [Then(@"the topic should be created successfully")]
    public void ThenTheTopicShouldBeCreatedSuccessfully()
    {
        _context.CreatedTopic.ShouldNotBeNull();
        _context.CreatedTopic.Name.ShouldNotBeNullOrEmpty();
    }

    [Then("the topic should have name {string}")]
    public void ThenTheTopicShouldHaveName(string expectedName)
    {
        _context.CreatedTopic!.Name.ShouldBe(expectedName);
    }

    [Then(@"the topic should have (.*) partitions")]
    public void ThenTheTopicShouldHavePartitions(uint expectedPartitions)
    {
        _context.CreatedTopic!.Partitions!.Count().ShouldBe((int)expectedPartitions);
        _context.CreatedTopic.PartitionsCount.ShouldBe(expectedPartitions);
    }

    [When(@"I send (\d+) messages to stream (\d+), topic (\d+), partition (\d+)")]
    public async Task WhenISendMessagesToStreamTopicPartition(int messageCount, int streamId, int topicId,
        int partitionId)
    {
        Message[] messages = Enumerable.Range(0, messageCount)
            .Select(i => new Message((UInt128)(i + 1), Encoding.UTF8.GetBytes($"Test message {i}")))
            .ToArray();

        await _context.IggyClient.SendMessagesAsync(Identifier.Numeric(streamId), Identifier.Numeric(topicId),
            Partitioning.PartitionId(partitionId), messages);

        _context.LastSendMessage = messages[^1];
    }

    [Then(@"all messages should be sent successfully")]
    public void ThenAllMessagesShouldBeSentSuccessfully()
    {
        _context.LastSendMessage.ShouldNotBeNull();
    }

    [When(@"I poll messages from stream (\d+), topic (\d+), partition (\d+) starting from offset (\d+)")]
    public async Task WhenIPollMessagesFromStreamTopicPartitionStartingFromOffset(int streamId, int topicId,
        uint partitionId, ulong startOffset)
    {
        var messages = await _context.IggyClient.PollMessagesAsync(new MessageFetchRequest
        {
            StreamId = Identifier.Numeric(streamId),
            TopicId = Identifier.Numeric(topicId),
            PartitionId = partitionId,
            PollingStrategy = PollingStrategy.Offset(startOffset),
            Consumer = Consumer.New(0),
            Count = 100,
            AutoCommit = false
        });

        _context.PolledMessages = messages.Messages.ToList();
    }

    [Then(@"I should receive (.*) messages")]
    public void ThenIShouldReceiveMessages(int expectedCount)
    {
        _context.PolledMessages.Count.ShouldBe(expectedCount);
    }

    [Then(@"the messages should have sequential offsets from (\d+) to (\d+)")]
    public void ThenTheMessagesShouldHaveSequentialOffsetsFromTo(int startOffset, int endOffset)
    {
        for (var i = startOffset; i < endOffset; i++)
        {
            _context.PolledMessages[i].Header.Offset.ShouldBe((ulong)i);
        }
    }

    [Then(@"each message should have the expected payload content")]
    public void ThenEachMessageShouldHaveTheExpectedPayloadContent()
    {
        for (var i = 0; i < _context.PolledMessages.Count; i++)
        {
            var message = _context.PolledMessages[i];
            message.Payload.ShouldNotBeNull();
            message.Payload.Length.ShouldBeGreaterThan(0);

            var payloadText = Encoding.UTF8.GetString(message.Payload);
            payloadText.ShouldBe($"Test message {i}");
        }
    }

    [Then(@"the last polled message should match the last sent message")]
    public void ThenTheLastPolledMessageShouldMatchTheLastSentMessage()
    {
        var lastPolled = _context.PolledMessages.LastOrDefault();

        lastPolled.ShouldNotBeNull();
        _context.LastSendMessage.ShouldNotBeNull();

        lastPolled.Header.Id.ShouldBe(_context.LastSendMessage.Header.Id);
        lastPolled.Payload.ShouldBe(_context.LastSendMessage.Payload);
    }
}

// Test context for sharing data between steps
