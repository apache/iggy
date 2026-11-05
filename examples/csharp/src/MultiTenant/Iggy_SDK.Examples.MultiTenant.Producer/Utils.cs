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
using Apache.Iggy.Messages;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Iggy_SDK.Examples.MultiTenant.Producer;

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
            var userName = $"tenant_{id}_producer";
            var (stream, topic) = await EnsureResourcesAsync(rootClient, streamName);
            await EnsureUserAsync(rootClient, userName, stream.Id, topic.Id, canProduce: true);
            tenants.Add(new Tenant(id, streamName, userName, stream.Id, topic.Id));
        }

        var clients = new List<IIggyClient>();
        try
        {
            foreach (var tenant in tenants)
            {
                var client = await CreateClientAsync(address, tenant.UserName, Password, loggerFactory);
                clients.Add(client);
                await VerifyIsolationAsync(client, tenant, tenants, logger);
            }

            foreach (var tenant in tenants)
            {
                var client = clients[tenants.IndexOf(tenant)];
                for (var batch = 1; batch <= BatchCount; batch++)
                {
                    var messages = Enumerable.Range(1, MessagesPerBatch)
                        .Select(message => new Message(
                            Guid.NewGuid(),
                            Encoding.UTF8.GetBytes($"tenant={tenant.Id};batch={batch};message={message}")))
                        .ToList();
                    await client.SendMessagesAsync(
                        Identifier.Numeric(tenant.StreamId),
                        Identifier.Numeric(tenant.TopicId),
                        Partitioning.PartitionId(0),
                        messages);
                }

                logger.LogInformation("Tenant {TenantId} sent {BatchCount} batches.", tenant.Id, BatchCount);
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

    private static async Task<(StreamResponse Stream, TopicResponse Topic)> EnsureResourcesAsync(
        IIggyClient client,
        string streamName)
    {
        var streamId = Identifier.String(streamName);
        var stream = await client.GetStreamByIdAsync(streamId) ??
                     await client.CreateStreamAsync(streamName) ??
                     throw new InvalidOperationException($"Could not create stream {streamName}.");
        var topicId = Identifier.String(TopicName);
        var topic = await client.GetTopicByIdAsync(Identifier.Numeric(stream.Id), topicId) ??
                    await client.CreateTopicAsync(Identifier.Numeric(stream.Id), TopicName, 1) ??
                    throw new InvalidOperationException($"Could not create topic {TopicName}.");
        return (stream, topic);
    }

    private static async Task EnsureUserAsync(
        IIggyClient client,
        string userName,
        uint streamId,
        uint topicId,
        bool canProduce)
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
                    PollMessages = false,
                    SendMessages = canProduce,
                    Topics = new Dictionary<uint, TopicPermissions>
                    {
                        [topicId] = new TopicPermissions
                        {
                            ManageTopic = false,
                            ReadTopic = true,
                            PollMessages = false,
                            SendMessages = canProduce
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

    private static async Task VerifyIsolationAsync(
        IIggyClient client,
        Tenant tenant,
        IReadOnlyList<Tenant> tenants,
        ILogger logger)
    {
        foreach (var other in tenants.Where(other => other.Id != tenant.Id))
        {
            try
            {
                var stream = await client.GetStreamByIdAsync(Identifier.Numeric(other.StreamId));
                if (stream is not null)
                {
                    throw new InvalidOperationException(
                        $"Tenant {tenant.Id} can access tenant {other.Id}'s stream.");
                }
            }
            catch (IggyInvalidStatusCodeException)
            {
                logger.LogInformation(
                    "Tenant {TenantId} cannot access tenant {OtherTenantId}'s stream.",
                    tenant.Id,
                    other.Id);
            }
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

    private sealed record Tenant(int Id, string StreamName, string UserName, uint StreamId, uint TopicId);
}
