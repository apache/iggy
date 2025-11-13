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
using System.Text.Json;
using Apache.Iggy;
using Apache.Iggy.Contracts;
using Apache.Iggy.Headers;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.MessageHeaders.Consumer;

public static class Utils
{
    private const string StreamName = "message-headers-example-stream";
    private const string TopicName = "message-headers-example-topic";
    private const uint PartitionId = 0;
    private const uint BatchesLimit = 5;

    public static async Task ConsumeMessages(IIggyClient client, ILogger logger)
    {
        var interval = TimeSpan.FromMilliseconds(500);
        logger.LogInformation(
            "Messages will be consumed from stream: {StreamId}, topic: {TopicId}, partition: {PartitionId} with interval {Interval}.",
            StreamName,
            TopicName,
            PartitionId,
            interval
        );

        var offset = 0ul;
        uint messagesPerBatch = 10;
        var consumedBatches = 0;
        var consumer = Apache.Iggy.Kinds.Consumer.New(1);
        while (true)
        {
            if (consumedBatches == BatchesLimit)
            {
                logger.LogInformation(
                    "Consumed {ConsumedBatches} batches of messages, exiting.",
                    consumedBatches
                );
                return;
            }

            var streamIdentifier = Identifier.String(StreamName);
            var topicIdentifier = Identifier.String(TopicName);
            var polledMessages = await client.PollMessagesAsync(
                streamIdentifier,
                topicIdentifier,
                PartitionId,
                consumer,
                PollingStrategy.Offset(offset),
                messagesPerBatch,
                false
            );

            if (!polledMessages.Messages.Any())
            {
                logger.LogInformation("No messages found.");
                await Task.Delay(interval);
                continue;
            }

            offset += (ulong)polledMessages.Messages.Count;
            foreach (var message in polledMessages.Messages) HandleMessage(message, logger);
            consumedBatches++;
            await Task.Delay(interval);
        }
    }

    private static void HandleMessage(MessageResponse message, ILogger logger)
    {
        var headerKey = HeaderKey.New("message_type");
        var headersMap = message.UserHeaders ?? throw new Exception("Missing headers map.");
        var messageType = Encoding.UTF8.GetString(headersMap[headerKey].Value);

        logger.LogInformation(
            "Handling message type: {MessageType} at offset: {Offset}...",
            messageType,
            message.Header.Offset
        );

        switch (messageType)
        {
            case Envelope.ORDER_CREATED_TYPE:
                var orderCreated = JsonSerializer.Deserialize<OrderCreated>(message.Payload) ??
                                   throw new Exception("Could not deserialize order_created.");
                logger.LogInformation("{OrderCreated}", orderCreated);
                break;

            case Envelope.ORDER_CONFIRMED_TYPE:
                var orderConfirmed = JsonSerializer.Deserialize<OrderConfirmed>(message.Payload) ??
                                     throw new Exception("Could not deserialize order_confirmed.");
                logger.LogInformation("{OrderConfirmed}", orderConfirmed);
                break;
            case Envelope.ORDER_REJECTED_TYPE:
                var orderRejected = JsonSerializer.Deserialize<OrderRejected>(message.Payload) ??
                                    throw new Exception("Could not deserialize order_rejected.");
                logger.LogInformation("{OrderRejected}", orderRejected);
                break;
            default:
                logger.LogWarning("Received unknown message type: {MessageType}", messageType);
                break;
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
        {
            throw new FormatException(
                $"Invalid argument {argumentName}! Usage: --tcp-server-address <server-address>"
            );
        }

        tcpServerAddr = tcpServerAddr ?? throw new ArgumentNullException(tcpServerAddr);
        if (!IPEndPoint.TryParse(tcpServerAddr, out _))
        {
            throw new FormatException(
                $"Invalid server address {tcpServerAddr}! Usage: --tcp-server-address <server-address>"
            );
        }

        logger.LogInformation("Using server address: {TcpServerAddr}", tcpServerAddr);
        return tcpServerAddr;
    }
}
