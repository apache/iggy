/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { Client, Consumer, PollingStrategy } from 'apache-iggy';
import { BATCHES_LIMIT, log, parseArgs } from '../utils';


interface TenantConsumer {
  tenantId: number;
  streamId: number;
  topicId: number;
  partitionId: number;
  offset: number;
  messagesConsumed: number;
}


async function discoverTenantStreams(client: Client): Promise<TenantConsumer[]> {
  const tenantConsumers: TenantConsumer[] = [];

  try {
    const streams = await client.stream.list();
    log('Discovered %d stream(s)', streams.length);

    for (const stream of streams) {
      if (stream.name.includes('tenant-')) {
        const topics = await client.topic.list({
          streamId: stream.id,
        });

        for (const topic of topics) {
          if (topic.partitions && topic.partitions.length > 0) {
            tenantConsumers.push({
              tenantId: parseInt(stream.name.split('-')[1], 10),
              streamId: stream.id,
              topicId: topic.id,
              partitionId: topic.partitions[0].id,
              offset: 0,
              messagesConsumed: 0,
            });
            log('Added consumer for tenant %s - stream %d, topic %d', stream.name, stream.id, topic.id);
          }
        }
      }
    }
  } catch (error) {
    log('Error discovering streams: %o', error);
  }

  return tenantConsumers;
}

async function consumeMessagesFromTenants(
  client: Client,
  tenantConsumers: TenantConsumer[]
): Promise<void> {
  const interval = 500; // 500 milliseconds
  const MESSAGES_PER_POLL = 10;

  log('Starting to consume messages from %d tenant(s)...', tenantConsumers.length);

  for (let batch = 0; batch < BATCHES_LIMIT; batch++) {
    log('Poll batch %d', batch + 1);

    for (const consumer of tenantConsumers) {
      try {
        log('Polling from tenant %d (stream %d, topic %d)...', consumer.tenantId, consumer.streamId, consumer.topicId);

        const polledMessages = await client.message.poll({
          streamId: consumer.streamId,
          topicId: consumer.topicId,
          consumer: Consumer.Single,
          partitionId: consumer.partitionId,
          pollingStrategy: PollingStrategy.Offset(BigInt(consumer.offset)),
          count: MESSAGES_PER_POLL,
          autocommit: false,
        });

        if (polledMessages && polledMessages.messages.length > 0) {
          consumer.offset += polledMessages.messages.length;

          for (const message of polledMessages.messages) {
            const payload = new TextDecoder().decode(new Uint8Array(Object.values(message.payload)));
            log('  [Tenant %d] %s', consumer.tenantId, payload);
            consumer.messagesConsumed++;
          }
          log('Consumed %d message(s) from tenant %d', polledMessages.messages.length, consumer.tenantId);
        } else {
          log('No messages available for tenant %d', consumer.tenantId);
        }
      } catch (error) {
        log('Error consuming messages for tenant %d: %o', consumer.tenantId, error);
      }
    }

    await new Promise(resolve => setTimeout(resolve, interval));
  }

  log('Finished consuming messages. Summary:');
  tenantConsumers.forEach(c => {
    log('  Tenant %d: %d messages consumed', c.tenantId, c.messagesConsumed);
  });
}

async function main() {
  const args = parseArgs();

  log('Using connection string: %s', args.connectionString);

  // Parse connection string (simplified parsing for this example)
  const url = new URL(args.connectionString.replace('iggy+tcp://', 'http://'));
  const host = url.hostname;
  const port = parseInt(url.port) || 8090;
  const username = url.username || 'iggy';
  const password = url.password || 'iggy';

  const client = new Client({
    transport: 'TCP',
    options: {
      port,
      host,
      keepAlive: true,
    },
    reconnect: {
      enabled: true,
      interval: 5000,
      maxRetries: 5,
    },
    heartbeatInterval: 5000,
    credentials: { username, password },
  });

  try {
    log('Multi-tenant consumer has started, selected transport: TCP');
    log('Connecting to Iggy server...');
    log('Connected successfully.');

    const tenantConsumers = await discoverTenantStreams(client);

    if (tenantConsumers.length === 0) {
      log('No tenant streams found. Make sure the producer has run first.');
      return;
    }

    await consumeMessagesFromTenants(client, tenantConsumers);
  } catch (error) {
    log('Error in main: %o', error);
    process.exitCode = 1;
  } finally {
    await client.destroy();
    log('Disconnected from server.');
  }
}

process.on('unhandledRejection', (reason, promise) => {
  log('Unhandled Rejection at: %o, reason: %o', promise, reason);
  process.exitCode = 1;
});

await main();
