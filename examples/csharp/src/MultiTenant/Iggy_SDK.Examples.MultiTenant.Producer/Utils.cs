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

using System.Text;
using Apache.Iggy;
using Apache.Iggy.IggyClient;
using Apache.Iggy.Messages;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;
using Partitioning = Apache.Iggy.Kinds.Partitioning;

namespace Iggy_SDK.Examples.MultiTenant.Producer;

public static class Utils
{
    private const int BatchesLimit = 5;
    private const int MessagesPerBatch = 10;

    public static string ProducerUsername(int tenantId)
    {
        return $"tenant_{tenantId}_producer";
    }

    public static async Task CreateStreamAndUser(IIggyClient root, int tenantId, ILogger logger)
    {
        var name = MultiTenantHelpers.StreamName(tenantId);
        var streamId = Identifier.String(name);
        await ExampleHelpers.EnsureStreamExists(root, streamId, name);

        // Permissions are keyed by the numeric stream ID, which EnsureStreamExists does not return.
        var stream = await root.GetStreamByIdAsync(streamId)
                     ?? throw new InvalidOperationException($"Stream {name} does not exist.");
        logger.LogInformation("Stream {Stream} is ready with ID {StreamId}.", name, stream.Id);

        // Managing topics includes reading them and sending messages.
        var permissions = MultiTenantHelpers.StreamOnlyPermissions(stream.Id, manageTopics: true);
        await MultiTenantHelpers.EnsureUser(root, ProducerUsername(tenantId), permissions, logger);
    }

    public static async Task ProduceMessages(IIggyClient client, int tenantId, ILogger logger)
    {
        var name = MultiTenantHelpers.StreamName(tenantId);
        var streamId = Identifier.String(name);
        var partitioning = Partitioning.PartitionId(MultiTenantHelpers.PARTITION_ID);

        foreach (var topic in MultiTenantHelpers.Topics)
        {
            var topicId = Identifier.String(topic);
            await ExampleHelpers.EnsureTopicExists(client, streamId, topicId, topic, 1);

            for (var batch = 0; batch < BatchesLimit; batch++)
            {
                var messages = Enumerable
                    .Range(batch * MessagesPerBatch + 1, MessagesPerBatch)
                    .Select(id => new Message(
                        Guid.NewGuid(),
                        Encoding.UTF8.GetBytes($"tenant-{tenantId}-{topic}-{id}")
                    ))
                    .ToList();

                await client.SendMessagesAsync(streamId, topicId, partitioning, messages);
            }

            logger.LogInformation(
                "Tenant {TenantId} sent {Count} messages to {Stream}/{Topic}.",
                tenantId,
                BatchesLimit * MessagesPerBatch,
                name,
                topic
            );
        }
    }
}
