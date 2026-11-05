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
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.MultiTenant.Consumer;

public static class Utils
{
    private const string TopicName = "events";
    private const string Password = "secret";
    private const int TenantCount = 3;
    private const int BatchCount = 5;
    private const int MessagesPerBatch = 10;

    public static async Task RunAsync(string address, ILoggerFactory loggerFactory, ILogger logger)
    {
        using var rootClient = await CreateClientAsync(address, "iggy", "iggy", loggerFactory);
        var tenants = new List<Tenant>();

        for (var id = 1; id <= TenantCount; id++)
        {
            var streamName = $"tenant_{id}_stream";
            var userName = $"tenant_{id}_consumer";
            var stream = await rootClient.GetStreamByIdAsync(Identifier.String(streamName)) ??
                         throw new InvalidOperationException($"Stream {streamName} does not exist.");
            var topic = await rootClient.GetTopicByIdAsync(
                            Identifier.Numeric(stream.Id),
                            Identifier.String(TopicName)) ??
                        throw new InvalidOperationException($"Topic {TopicName} does not exist.");
            await EnsureUserAsync(rootClient, userName, stream.Id, topic.Id);
            tenants.Add(new Tenant(id, userName, stream.Id, topic.Id));
        }

        var clients = new List<IIggyClient>();
        try
        {
            foreach (var tenant in tenants)
            {
                clients.Add(await CreateClientAsync(address, tenant.UserName, Password, loggerFactory));
            }

            for (var index = 0; index < tenants.Count; index++)
            {
                var tenant = tenants[index];
                var client = clients[index];
                var offset = 0ul;
                for (var batch = 1; batch <= BatchCount; batch++)
                {
                    var polled = await client.PollMessagesAsync(
                        Identifier.Numeric(tenant.StreamId),
                        Identifier.Numeric(tenant.TopicId),
                        0,
                        Apache.Iggy.Kinds.Consumer.New(1),
                        Apache.Iggy.Kinds.PollingStrategy.Offset(offset),
                        MessagesPerBatch,
                        false);
                    if (polled.Messages.Count == 0)
                    {
                        logger.LogInformation("Tenant {TenantId} has no messages at batch {Batch}.", tenant.Id, batch);
                        break;
                    }

                    foreach (var message in polled.Messages)
                    {
                        logger.LogInformation(
                            "Tenant {TenantId} received: {Payload}",
                            tenant.Id,
                            Encoding.UTF8.GetString(message.Payload));
                        offset = message.Header.Offset + 1;
                    }
                }

                logger.LogInformation("Tenant {TenantId} consumed its stream.", tenant.Id);
            }
        }
        finally
        {
            foreach (var client in clients)
            {
                client.Dispose();
            }
        }
    }

    private static async Task EnsureUserAsync(
        IIggyClient client,
        string userName,
        uint streamId,
        uint topicId)
    {
        var permissions = new Permissions
        {
            Global = EmptyGlobalPermissions(),
            Streams = new Dictionary<uint, StreamPermissions>
            {
                [streamId] = new StreamPermissions
                {
                    ManageStream = false,
                    ReadStream = true,
                    ManageTopics = false,
                    ReadTopics = true,
                    PollMessages = true,
                    SendMessages = false,
                    Topics = new Dictionary<uint, TopicPermissions>
                    {
                        [topicId] = new TopicPermissions
                        {
                            ManageTopic = false,
                            ReadTopic = true,
                            PollMessages = true,
                            SendMessages = false
                        }
                    }
                }
            }
        };
        var userId = Identifier.String(userName);
        if (await client.GetUserAsync(userId) is null)
        {
            await client.CreateUserAsync(userName, Password, UserStatus.Active, permissions);
        }
        else
        {
            await client.UpdatePermissionsAsync(userId, permissions);
        }
    }

    private static async Task<IIggyClient> CreateClientAsync(
        string address,
        string userName,
        string password,
        ILoggerFactory loggerFactory)
    {
        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = address,
            Protocol = Protocol.Tcp,
            LoggerFactory = loggerFactory
        });
        await client.ConnectAsync();
        await client.LoginUserAsync(userName, password);
        return client;
    }

    private static GlobalPermissions EmptyGlobalPermissions() => new()
    {
        ManageServers = false,
        ReadServers = false,
        ManageUsers = false,
        ReadUsers = false,
        ManageStreams = false,
        ReadStreams = false,
        ManageTopics = false,
        ReadTopics = false,
        PollMessages = false,
        SendMessages = false
    };

    public static string GetTcpServerAddr(string[] args, ILogger logger)
    {
        var defaultServerAddr = "127.0.0.1:8090";
        var argumentName = args.Length > 0 ? args[0] : null;
        var tcpServerAddr = args.Length > 1 ? args[1] : null;
        if (argumentName is null && tcpServerAddr is null)
        {
            return defaultServerAddr;
        }

        argumentName = argumentName ?? throw new ArgumentNullException(nameof(argumentName));
        if (argumentName != "--tcp-server-address")
        {
            throw new FormatException(
                $"Invalid argument {argumentName}! Usage: --tcp-server-address <server-address>");
        }

        tcpServerAddr = tcpServerAddr ?? throw new ArgumentNullException(nameof(tcpServerAddr));
        if (!IPEndPoint.TryParse(tcpServerAddr, out _))
        {
            throw new FormatException(
                $"Invalid server address {tcpServerAddr}! Usage: --tcp-server-address <server-address>");
        }

        logger.LogInformation("Using server address: {TcpServerAddr}", tcpServerAddr);
        return tcpServerAddr;
    }

    private sealed record Tenant(int Id, string UserName, uint StreamId, uint TopicId);
}
