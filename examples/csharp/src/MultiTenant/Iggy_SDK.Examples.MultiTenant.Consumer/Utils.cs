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
using Apache.Iggy.Kinds;
using Iggy_SDK.Examples.Shared;
using Microsoft.Extensions.Logging;

namespace Iggy_SDK.Examples.MultiTenant.Consumer;

public static class Utils
{
    private const string ConsumerName = "multi-tenant";
    private const uint MessagesPerPoll = 100;

    public static string ConsumerUsername(int tenantId)
    {
        return $"tenant_{tenantId}_consumer";
    }

    public static async Task CreateConsumerUser(IIggyClient root, int tenantId, ILogger logger)
    {
        var name = MultiTenantHelpers.StreamName(tenantId);
        var stream = await root.GetStreamByIdAsync(Identifier.String(name))
                     ?? throw new InvalidOperationException($"Stream {name} does not exist, run the producer first.");

        // Reading the stream includes polling its topics, storing consumer offsets,
        // and creating or deleting consumer groups in the stream.
        // It does not allow sending messages or changing topics.
        var permissions = MultiTenantHelpers.StreamOnlyPermissions(stream.Id, manageTopics: false);
        await MultiTenantHelpers.EnsureUser(root, ConsumerUsername(tenantId), permissions, logger);
    }

    public static async Task ConsumeMessages(IIggyClient client, int tenantId, ILogger logger)
    {
        var name = MultiTenantHelpers.StreamName(tenantId);
        var streamId = Identifier.String(name);
        var consumer = Apache.Iggy.Kinds.Consumer.New(ConsumerName);

        foreach (var topic in MultiTenantHelpers.Topics)
        {
            var topicId = Identifier.String(topic);
            var consumed = 0;

            // The server stores the offset after each poll, so the next poll and the next run
            // continue after the last message read. An empty poll ends the loop.
            while (true)
            {
                var polled = await client.PollMessagesAsync(
                    streamId,
                    topicId,
                    MultiTenantHelpers.PARTITION_ID,
                    consumer,
                    PollingStrategy.Next(),
                    MessagesPerPoll,
                    autoCommit: true
                );

                if (polled.Messages.Count == 0)
                {
                    break;
                }

                foreach (var message in polled.Messages)
                {
                    logger.LogInformation(
                        "Tenant {TenantId} received {Payload} from {Stream}/{Topic} at offset {Offset}.",
                        tenantId,
                        Encoding.UTF8.GetString(message.Payload),
                        name,
                        topic,
                        message.Header.Offset
                    );
                }

                consumed += polled.Messages.Count;
            }

            logger.LogInformation(
                "Tenant {TenantId} consumed {Consumed} messages from {Topic}.",
                tenantId,
                consumed,
                topic
            );
        }
    }
}
