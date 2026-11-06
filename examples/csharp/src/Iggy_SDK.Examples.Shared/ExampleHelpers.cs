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
using Apache.Iggy;
using Apache.Iggy.Exceptions;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.Shared;

public static class ExampleHelpers
{
    private const int StreamNameAlreadyExists = 1012;
    private const int TopicNameAlreadyExists = 2013;

    public static async Task EnsureStreamExists(
        IIggyClient client,
        Identifier streamId,
        string streamName,
        CancellationToken token = default
    )
    {
        var stream = await client.GetStreamByIdAsync(streamId, token);
        if (stream == null)
        {
            try
            {
                await client.CreateStreamAsync(streamName, token: token);
            }
            catch (IggyInvalidStatusCodeException error) when (error.FromServer && error.StatusCode == StreamNameAlreadyExists)
            {
            }
        }
    }

    public static async Task EnsureTopicExists(
        IIggyClient client,
        Identifier streamId,
        Identifier topicId,
        string topicName,
        uint partitionsCount,
        CancellationToken cancellationToken = default
    )
    {
        var topic = await client.GetTopicByIdAsync(streamId, topicId, cancellationToken);
        if (topic == null)
        {
            try
            {
                await client.CreateTopicAsync(
                    streamId,
                    topicName,
                    partitionsCount,
                    token: cancellationToken
                );
            }
            catch (IggyInvalidStatusCodeException error) when (error.FromServer && error.StatusCode == TopicNameAlreadyExists)
            {
            }
        }
    }

    public static string GetTcpServerAddr(string[] args, ILogger logger)
    {
        var defaultServerAddr = "127.0.0.1:8090";
        if (args.Length == 0)
        {
            return defaultServerAddr;
        }

        var argumentName = args[0];
        if (argumentName != "--tcp-server-address")
        {
            throw new FormatException(
                $"Invalid argument {argumentName}! Usage: --tcp-server-address <server-address>"
            );
        }

        if (args.Length < 2)
        {
            throw new FormatException("Missing server address! Usage: --tcp-server-address <server-address>");
        }

        var tcpServerAddr = args[1];
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
