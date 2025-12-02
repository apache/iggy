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
import { log, sleep } from '../utils';
import crypto from 'crypto';


async function buildClientAndStream(connectionString: string) {
  log('Building Iggy client and connecting...');

  // Parse connection string
  const url = new URL(connectionString.replace('iggy+tcp://', 'http://'));
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

  log('Creating stream and topic...');
  const streamName = `stream-${crypto.randomBytes(4).toString('hex')}`;
  const topicName = `topic-${crypto.randomBytes(4).toString('hex')}`;

  const stream = await client.stream.create({
    name: streamName,
  });

  const topic = await client.topic.create({
    streamId: stream.id,
    name: topicName,
    partitionCount: 1,
    compressionAlgorithm: 1,
    replicationFactor: 1,
  });

  log(`Stream created: ${stream.id}`);
  log(`Topic created: ${topic.id}`);

  return { client, stream, topic };
}

async function produceAndConsume(client: Client, stream: any, topic: any) {
  log('Starting producer and consumer...');

  log('Sending 3 test messages...');
  const messages = [
    { payload: 'Hello World' },
    { payload: 'Hola Iggy' },
    { payload: 'Hi Apache' },
  ];

  await client.message.send({
    streamId: stream.id,
    topicId: topic.id,
    messages,
    partition: Partitioning.PartitionId(topic.partitions[0].id),
  });

  log('Messages sent, waiting for consumption...');
  await sleep(500);

  log('Polling messages...');
  const polledMessages = await client.message.poll({
    streamId: stream.id,
    topicId: topic.id,
    consumer: { kind: 1, id: 1 }, // Consumer.Single
    partitionId: topic.partitions[0].id,
    pollingStrategy: { kind: 1, value: 0n }, // Offset(0)
    count: 10,
    autocommit: false,
  });

  if (polledMessages && polledMessages.messages.length > 0) {
    log(`Consumed ${polledMessages.messages.length} messages:`);
    for (const message of polledMessages.messages) {
      const payload = new TextDecoder().decode(new Uint8Array(Object.values(message.payload)));
      log(`  - ${payload}`);
    }
  }
}

async function cleanup(client: Client, stream: any, topic: any) {
  log('Cleaning up...');
  try {
    await client.topic.delete({
      streamId: stream.id,
      topicId: topic.id,
      partitionsCount: 1,
    });
    log('Topic deleted');
  } catch (error) {
    log(`Error deleting topic: ${error}`);
  }

  try {
    await client.stream.delete({ streamId: stream.id });
    log('Stream deleted');
  } catch (error) {
    log(`Error deleting stream: ${error}`);
  }
}

async function main() {
  const connectionString = process.argv[2] || 'iggy+tcp://iggy:iggy@127.0.0.1:8090';

  try {
    log('Stream Builder Example');
    const { client, stream, topic } = await buildClientAndStream(connectionString);

    await produceAndConsume(client, stream, topic);

    await cleanup(client, stream, topic);

    await client.destroy();
    log('Disconnected from server');
  } catch (error) {
    log(`Error: ${error}`);
    process.exitCode = 1;
  }
}

await main();
