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
using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Apache.Iggy.Messages;

namespace Apache.Iggy.Benchmarks;

public static class SendMessage
{
    public static async Task Create(IIggyClient bus, int producerNumber, int producerCount,
        int messagesBatch, int messagesCount, int messageSize, Identifier streamId, Identifier topicId)
    {
        long totalMessages = messagesBatch * messagesCount;
        var totalMessagesBytes = totalMessages * messageSize;
        Console.WriteLine(
            $"Executing Send Messages command for producer {producerNumber}, stream id {streamId}, messages count {totalMessages}, with size {totalMessagesBytes}");
        Message[] messages = CreateMessages(messagesCount, messageSize);
        List<TimeSpan> latencies = new();

        for (var i = 0; i < messagesBatch; i++)
        {
            var startTime = Stopwatch.GetTimestamp();
            await bus.SendMessagesAsync(new MessageSendRequest
            {
                StreamId = streamId,
                TopicId = topicId,
                Partitioning = Partitioning.PartitionId(1),
                Messages = messages
            });
            var diff = Stopwatch.GetElapsedTime(startTime);
            latencies.Add(diff);
        }

        var totalLatencies = latencies.Sum(x => x.TotalSeconds);
        var avgLatency = Math.Round(latencies.Sum(x => x.TotalMilliseconds) / (producerCount * latencies.Count), 2);
        var duration = totalLatencies / producerCount;
        var avgThroughput = Math.Round(totalMessagesBytes / duration / 1024.0 / 1024.0, 2);

        Console.WriteLine($"Total message bytes: {totalMessagesBytes}, average latency: {avgLatency} ms.");
        Console.WriteLine(
            $"Producer number: {producerNumber} send Messages: {messagesCount} in {messagesBatch} batches, with average throughput {avgThroughput} MB/s");
    }

    private static Message[] CreateMessages(int messagesCount, int messageSize)
    {
        var messages = new Message[messagesCount];
        for (var i = 0; i < messagesCount; i++)
        {
            messages[i] = new Message(Guid.NewGuid(), CreatePayload(messageSize));
        }

        return messages;
    }

    private static byte[] CreatePayload(int size)
    {
        var payloadBuilder = new StringBuilder(size);
        for (uint i = 0; i < size; i++)
        {
            var character = (char)(i % 26 + 97);
            payloadBuilder.Append(character);
        }

        var payloadString = payloadBuilder.ToString();
        return Encoding.UTF8.GetBytes(payloadString);
    }
}