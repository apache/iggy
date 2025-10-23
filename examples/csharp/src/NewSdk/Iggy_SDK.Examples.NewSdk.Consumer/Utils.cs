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
using Apache.Iggy.Consumers;
using Apache.Iggy.Contracts;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Kinds;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.NewSdk.Consumer;

public static class Utils
{
    public static void HandleMessage(ReceivedMessage message, ILogger logger)
    {
        var payload = Encoding.UTF8.GetString(message.Message.Payload);
        var envelope = JsonSerializer.Deserialize<Envelope>(payload) ??
                       throw new Exception("Could not deserialize envelope.");

        logger.LogInformation(
            "[{Partition}] Handling message type: {MessageType} at offset: {Offset}",
            message.PartitionId,
            envelope.MessageType,
            message.CurrentOffset
        );

        switch (envelope.MessageType)
        {
            case Envelope.OrderCreatedType:
                var orderCreated = JsonSerializer.Deserialize<OrderCreated>(envelope.Payload) ??
                                   throw new Exception("Could not deserialize order_created.");
                logger.LogInformation("{OrderCreated}", orderCreated);
                break;

            case Envelope.OrderConfirmedType:
                var orderConfirmed = JsonSerializer.Deserialize<OrderConfirmed>(envelope.Payload) ??
                                     throw new Exception("Could not deserialize order_confirmed.");
                logger.LogInformation("{OrderConfirmed}", orderConfirmed);
                break;
            case Envelope.OrderRejectedType:
                var orderRejected = JsonSerializer.Deserialize<OrderRejected>(envelope.Payload) ??
                                    throw new Exception("Could not deserialize order_rejected.");
                logger.LogInformation("{OrderRejected}", orderRejected);
                break;
            default:
                logger.LogWarning("Received unknown message type: {MessageType}", envelope.MessageType);
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
