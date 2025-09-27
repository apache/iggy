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
using Apache.Iggy;
using Apache.Iggy.Contracts;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Iggy_SDK.Examples.GettingStarted.Producer;

public static class Utils
{
    private const uint STREAM_ID = 1;
    private const uint TOPIC_ID = 2;
    private const uint PARTITION_ID = 1;
    private const uint BATCHES_LIMIT = 5;

    public static async Task InitSystem(IIggyClient client, ILogger logger)
    {
        try
        {
            await client.CreateStreamAsync("getting-started-example-stream", STREAM_ID);
            logger.LogInformation("Stream was created.");
        }
        catch (InvalidResponseException)
        {
            logger.LogWarning("Stream already exists and will not be created again.");
        }

        try
        {
            await client.CreateTopicAsync(
                Identifier.Numeric(STREAM_ID),
                "getting-started-example-topic",
                1,
                CompressionAlgorithm.None,
                TOPIC_ID
            );
            logger.LogInformation("Topic was created.");
        }
        catch (InvalidResponseException)
        {
            logger.LogWarning("Topic already exists and will not be created again.");
        }
    }

    public static async Task ProduceMessages(IIggyClient client, ILogger logger)
    {
        var interval = TimeSpan.FromMilliseconds(500);
        logger.LogInformation(
            "Messages will be sent to stream: {StreamId}, topic: {TopicId}, partition: {PartitionId} with interval {Interval}.",
            STREAM_ID,
            TOPIC_ID,
            PARTITION_ID,
            interval
        );

        var currentId = 0;
        var messagesPerBatch = 10;
        var sentBatches = 0;
        var partitioning = Partitioning.PartitionId((int)PARTITION_ID); // should be uint
        while (true)
        {
            if (sentBatches == BATCHES_LIMIT)
            {
                logger.LogInformation(
                    "Sent {SentBatches} batches of messages, exiting.",
                    sentBatches
                );
                return;
            }

            var payloads = Enumerable
                .Range(currentId, messagesPerBatch)
                .Aggregate(new List<string>(), (list, next) =>
                {
                    list.Add($"message-{next}");
                    return list;
                });

            var messages = payloads.Select(payload => new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(payload)))
                .ToList();

            var streamIdentifier = Identifier.Numeric(STREAM_ID);
            var topicIdentifier = Identifier.Numeric(TOPIC_ID);
            await client.SendMessagesAsync(
                new MessageSendRequest
                {
                    StreamId = streamIdentifier,
                    TopicId = topicIdentifier,
                    Partitioning = partitioning,
                    Messages = messages
                }
            );

            currentId += messagesPerBatch;
            sentBatches++;
            logger.LogInformation("Sent messages: {Messages}.", payloads);

            await Task.Delay(interval);
        }
    }

    public static string GetTcpServerAddr(string[] args, ILogger logger)
    {
        var defaultServerAddr = "127.0.0.1:8090";
        var argumentName = args.Length > 0 ? args[0] : null;
        var tcpServerAddr = args.Length > 1 ? args[1] : null;

        if (argumentName is null && tcpServerAddr is null) return defaultServerAddr;

        argumentName = argumentName ?? throw new ArgumentNullException(argumentName);
        if (argumentName != "--tcp-server-address")
            throw new FormatException(
                $"Invalid argument {argumentName}! Usage: --tcp-server-address <server-address>"
            );
        tcpServerAddr = tcpServerAddr ?? throw new ArgumentNullException(tcpServerAddr);
        if (!IPEndPoint.TryParse(tcpServerAddr, out _))
            throw new FormatException(
                $"Invalid server address {tcpServerAddr}! Usage: --tcp-server-address <server-address>"
            );
        logger.LogInformation("Using server address: {TcpServerAddr}", tcpServerAddr);
        return tcpServerAddr;
    }
}
