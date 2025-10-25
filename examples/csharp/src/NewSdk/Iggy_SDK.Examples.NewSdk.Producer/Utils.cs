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
using Apache.Iggy.Messages;
using Apache.Iggy.Publishers;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.NewSdk.Producer;

public static class Utils
{
    private const uint BATCHES_LIMIT = 5;

    public static async Task ProduceMessages(IggyPublisher publisher, ILogger logger)
    {
        var interval = TimeSpan.FromMilliseconds(500);
        logger.LogInformation(
            "Messages will be sent to stream: {StreamId}, topic: {TopicId}} with interval {Interval}.",
            publisher.StreamId,
            publisher.TopicId,
            interval
        );

        var messagesPerBatch = 10;
        var sentBatches = 0;
        var messagesGenerator = new MessagesGenerator();

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

            List<ISerializableMessage> serializableMessages = Enumerable
                .Range(0, messagesPerBatch)
                .Aggregate(new List<ISerializableMessage>(), (list, _) =>
                {
                    var serializableMessage = messagesGenerator.Generate();
                    list.Add(serializableMessage);
                    return list;
                });

            List<Message> messages = serializableMessages.Select(serializableMessage =>
                {
                    var jsonEnvelope = serializableMessage.ToJsonEnvelope();
                    return new Message(Guid.NewGuid(), Encoding.UTF8.GetBytes(jsonEnvelope));
                }
            ).ToList();

            logger.LogInformation("Sending messages count: {Count}", messagesPerBatch);

            await publisher.SendMessages(messages.ToArray());

            sentBatches++;
            logger.LogInformation("Sent messages: {Messages}.", serializableMessages);

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
