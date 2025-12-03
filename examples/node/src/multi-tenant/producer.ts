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

import { Client, Partitioning } from 'apache-iggy';
import { BATCHES_LIMIT, log, MESSAGES_PER_BATCH } from '../utils';
import crypto from 'crypto';

const TOPICS = ['events', 'logs', 'notifications'];


interface TenantProducer {
  tenantId: number;
  streamId: number;
  topicId: number;
  partitionId: number;
  messageCount: number;
}

function parseArgs() {
  const args = process.argv.slice(2);
  const connectionString = args[0] || 'iggy+tcp://iggy:iggy@127.0.0.1:8090';
  const tenantsCount = parseInt(args[1] || '2', 10);
  const producersPerTenant = parseInt(args[2] || '1', 10);

  if (args.length > 0 && (args[0] === '-h' || args[0] === '--help')) {
    log('Usage: node producer.ts [connection_string] [tenants_count] [producers_per_tenant]');
    log('Example: node producer.ts iggy+tcp://iggy:iggy@127.0.0.1:8090 2 1');
    process.exit(0);
  }

  return { connectionString, tenantsCount, producersPerTenant };
}

async function setupTenants(
  client: Client,
  tenantsCount: number,
  producersPerTenant: number
): Promise<TenantProducer[]> {
  const tenantProducers: TenantProducer[] = [];

  for (let tenantId = 1; tenantId <= tenantsCount; tenantId++) {
    const streamName = `tenant-${tenantId}-stream-${crypto.randomBytes(4).toString('hex')}`;
    log('Creating stream for tenant %d: %s', tenantId, streamName);

    const stream = await client.stream.create({
      name: streamName,
    });

    log('Stream created for tenant %d with ID: %d', tenantId, stream.id);

    for (let producerIdx = 0; producerIdx < producersPerTenant; producerIdx++) {
      const topicIdx = producerIdx % TOPICS.length;
      const topicName = `${TOPICS[topicIdx]}-${crypto.randomBytes(4).toString('hex')}`;

      log('Creating topic %s for tenant %d', topicName, tenantId);

      const topic = await client.topic.create({
        streamId: stream.id,
        name: topicName,
        partitionCount: 2,
        compressionAlgorithm: 1, // None
        replicationFactor: 1,
      });

      log('Topic created for tenant %d with ID: %d', tenantId, topic.id);

      tenantProducers.push({
        tenantId,
        streamId: stream.id,
        topicId: topic.id,
        partitionId: topic.partitions[0].id,
        messageCount: 0,
      });
    }
  }

  return tenantProducers;
}

async function produceMessagesForTenants(
  client: Client,
  tenantProducers: TenantProducer[]
): Promise<void> {
  const interval = 500; // 500 milliseconds

  log('Starting to produce messages for %d tenant(s)...', tenantProducers.length);

  for (let batch = 0; batch < BATCHES_LIMIT; batch++) {
    log('Sending batch %d', batch + 1);

    for (const producer of tenantProducers) {
      const messages = Array.from({ length: MESSAGES_PER_BATCH }).map((_, idx) => ({
        payload: `Tenant ${producer.tenantId} - Message ${producer.messageCount + idx + 1}`,
      }));

      try {
        await client.message.send({
          streamId: producer.streamId,
          topicId: producer.topicId,
          messages,
          partition: Partitioning.PartitionId(producer.partitionId),
        });

        producer.messageCount += MESSAGES_PER_BATCH;
        log('Sent %d messages for tenant %d', messages.length, producer.tenantId);
      } catch (error) {
        log('Error sending messages for tenant %d: %o', producer.tenantId, error);
      }
    }

    await new Promise(resolve => setTimeout(resolve, interval));
  }

  log('Finished producing messages. Total messages sent per tenant:');
  tenantProducers.forEach(p => {
    log('  Tenant %d: %d messages', p.tenantId, p.messageCount);
  });
}

async function cleanupTenants(client: Client, tenantProducers: TenantProducer[]): Promise<void> {
  log('Cleaning up resources...');

  const uniqueStreams = new Set(tenantProducers.map(p => p.streamId));

  for (const streamId of uniqueStreams) {
    try {
      await client.stream.delete({ streamId });
      log('Stream %d deleted successfully.', streamId);
    } catch (error) {
      log('Error deleting stream %d: %o', streamId, error);
    }
  }
}

async function main() {
  const args = parseArgs();

  log('Using connection string: %s', args.connectionString);
  log('Configuration - Tenants: %d, Producers per tenant: %d', args.tenantsCount, args.producersPerTenant);

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

  let tenantProducers: TenantProducer[] = [];

  try {
    log('Multi-tenant producer has started, selected transport: TCP');
    log('Connecting to Iggy server...');
    log('Connected successfully.');

    tenantProducers = await setupTenants(client, args.tenantsCount, args.producersPerTenant);
    await produceMessagesForTenants(client, tenantProducers);
  } catch (error) {
    log('Error in main: %o', error);
    process.exitCode = 1;
  } finally {
    if (tenantProducers.length > 0) {
      await cleanupTenants(client, tenantProducers);
    }
    await client.destroy();
    log('Disconnected from server.');
  }
}

process.on('unhandledRejection', (reason, promise) => {
  log('Unhandled Rejection at: %o, reason: %o', promise, reason);
  process.exitCode = 1;
});

await main();
