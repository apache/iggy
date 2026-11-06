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

using Apache.Iggy;
using Apache.Iggy.Configuration;
using Apache.Iggy.Contracts;
using Apache.Iggy.Contracts.Auth;
using Apache.Iggy.Enums;
using Apache.Iggy.Exceptions;
using Apache.Iggy.Factory;
using Apache.Iggy.IggyClient;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.Shared;

public static class MultiTenantHelpers
{
    public const uint PARTITION_ID = 0;

    // One fixed password keeps the example short and lets it run again against the same server.
    // Real tenants would each get their own secret.
    private const string TenantPassword = "secret";
    private const int Unauthorized = 41;
    private const int UserAlreadyExists = 46;

    public static readonly string[] Topics = ["events", "logs", "notifications"];

    public static string StreamName(int tenantId)
    {
        return $"tenant_{tenantId}_stream";
    }

    public static int ReadPositiveInt(string name, int defaultValue)
    {
        var value = Environment.GetEnvironmentVariable(name);
        if (value is null)
        {
            return defaultValue;
        }

        if (int.TryParse(value, out var parsed) && parsed > 0)
        {
            return parsed;
        }

        throw new FormatException($"{name} must be a positive integer, got '{value}'.");
    }

    public static async Task<IIggyClient> Connect(
        string address,
        string username,
        string password,
        ILoggerFactory loggerFactory
    )
    {
        var client = IggyClientFactory.CreateClient(new IggyClientConfigurator
        {
            BaseAddress = address,
            Protocol = Protocol.Tcp,
            LoggerFactory = loggerFactory,
            // The SDK retries forever by default, so the example would hang when no server is running.
            ReconnectionSettings = new ReconnectionSettings { MaxRetries = 2 }
        });

        await client.ConnectAsync();
        await client.LoginUserAsync(username, password);
        return client;
    }

    public static Task<IIggyClient> ConnectAsTenant(string address, string username, ILoggerFactory loggerFactory)
    {
        return Connect(address, username, TenantPassword, loggerFactory);
    }

    // The user gets no global permissions and no other stream, so other tenants' streams are closed to it.
    public static Permissions StreamOnlyPermissions(uint streamId, bool manageTopics)
    {
        return new Permissions
        {
            Global = new GlobalPermissions
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
            },
            Streams = new Dictionary<uint, StreamPermissions>
            {
                [streamId] = new()
                {
                    ManageStream = false,
                    ReadStream = true,
                    ManageTopics = manageTopics,
                    ReadTopics = false,
                    PollMessages = false,
                    SendMessages = false
                }
            }
        };
    }

    public static async Task EnsureUser(IIggyClient root, string username, Permissions permissions, ILogger logger)
    {
        var userId = Identifier.String(username);
        if (await root.GetUserAsync(userId) is null)
        {
            try
            {
                await root.CreateUserAsync(username, TenantPassword, UserStatus.Active, permissions);
                logger.LogInformation("Created user {Username}.", username);
                return;
            }
            catch (IggyInvalidStatusCodeException error) when (error.FromServer && error.StatusCode == UserAlreadyExists)
            {
            }
        }

        // Deleting a stream removes its grants, even if it is created again under the same ID,
        // so the stored permissions are replaced, not trusted.
        await root.UpdatePermissionsAsync(userId, permissions);
        logger.LogWarning("User {Username} already exists, its permissions were updated.", username);
    }

    public static async Task EnsureStreamAccess(IIggyClient client, int tenantId, int tenantsCount, ILogger logger)
    {
        var ownStream = StreamName(tenantId);
        if (await client.GetStreamByIdAsync(Identifier.String(ownStream)) is null)
        {
            throw new InvalidOperationException($"Tenant {tenantId} cannot access stream {ownStream}.");
        }

        logger.LogInformation("Tenant {TenantId} can access stream {Stream}.", tenantId, ownStream);

        for (var otherId = 1; otherId <= tenantsCount; otherId++)
        {
            if (otherId == tenantId)
            {
                continue;
            }

            var otherStream = StreamName(otherId);
            try
            {
                if (await client.GetStreamByIdAsync(Identifier.String(otherStream)) is not null)
                {
                    throw new InvalidOperationException(
                        $"Tenant {tenantId} must not have access to stream {otherStream}."
                    );
                }

                logger.LogInformation(
                    "Tenant {TenantId} has no access to stream {Stream} (not visible).",
                    tenantId,
                    otherStream
                );
            }
            catch (IggyInvalidStatusCodeException error) when (error.FromServer && error.StatusCode == Unauthorized)
            {
                logger.LogInformation(
                    "Tenant {TenantId} has no access to stream {Stream} (unauthorized).",
                    tenantId,
                    otherStream
                );
            }
        }
    }
}
