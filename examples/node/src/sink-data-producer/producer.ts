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

const SOURCES = ['browser', 'mobile', 'desktop', 'email', 'network', 'other'];
const STATES = ['active', 'inactive', 'blocked', 'deleted', 'unknown'];
// const DOMAINS = ['gmail.com', 'yahoo.com', 'hotmail.com', 'outlook.com', 'aol.com'];

interface Record {
  user_id: string;
  created_at: string;
  login_at: string;
  source: string;
  state: string;
}



function randomRecord(): Record {
  const userId = `user-${crypto.randomBytes(8).toString('hex')}`;
  const now = new Date();
  const daysAgo = Math.floor(Math.random() * 30);
  const createdAt = new Date(now.getTime() - daysAgo * 24 * 60 * 60 * 1000);

  const source = SOURCES[Math.floor(Math.random() * SOURCES.length)];
  const state = STATES[Math.floor(Math.random() * STATES.length)];
  // const domain = DOMAINS[Math.floor(Math.random() * DOMAINS.length)];

  return {
    user_id: userId,
    created_at: createdAt.toISOString(),
    login_at: now.toISOString(),
    source,
    state,
  };
}

function parseArgs() {
  const args = process.argv.slice(2);
  const address = args[0] || 'localhost:8090';
  const stream = args[1] || 'qw';
  const topic = args[2] || 'records';

  return { address, stream, topic };
}

async function createClient(address: string) {
  const parts = address.split(':');
  const host = parts[0] || 'localhost';
  const port = parseInt(parts[1]) || 8090;

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
    credentials: { username: 'iggy', password: 'iggy' },
  });

  return client;
}

async function produceData(client: Client, streamName: string, topicName: string) {
  log(`Starting data producer...`);
  log(`Stream: ${streamName}, Topic: ${topicName}`);

  // Get or create stream
  let stream;
  try {
    const streams = await client.stream.list();
    const existing = streams.find(s => s.name === streamName);
    if (existing) {
      stream = existing;
    } else {
      stream = await client.stream.create({ name: streamName });
    }
  } catch (error) {
    log(`Error getting or creating stream: ${error}`);
    stream = await client.stream.create({ name: streamName });
  }

  // Get or create topic
  let topic;
  try {
    const topics = await client.topic.list({ streamId: stream.id });
    const existing = topics.find(t => t.name === topicName);
    if (existing) {
      topic = existing;
    } else {
      topic = await client.topic.create({
        streamId: stream.id,
        name: topicName,
        partitionCount: 1,
        compressionAlgorithm: 1,
        replicationFactor: 1,
      });
    }
  } catch (error) {
    log(`Error getting or creating topic: ${error}`);
    topic = await client.topic.create({
      streamId: stream.id,
      name: topicName,
      partitionCount: 1,
      compressionAlgorithm: 1,
      replicationFactor: 1,
    });
  }

  log(`Stream ID: ${stream.id}, Topic ID: ${topic.id}`);

  let batchesCount = 0;
  const maxBatches = 100;

  while (batchesCount < maxBatches) {
    const recordsCount = Math.floor(Math.random() * 400) + 100; // 100-500
    const messages = Array.from({ length: recordsCount }).map(() => {
      const record = randomRecord();
      return {
        payload: JSON.stringify(record),
      };
    });

    try {
      await client.message.send({
        streamId: stream.id,
        topicId: topic.id,
        messages,
        partition: Partitioning.PartitionId(topic.partitions[0].id),
      });

      log(`Sent ${recordsCount} messages (batch ${batchesCount + 1}/${maxBatches})`);
    } catch (error) {
      log(`Error sending messages: ${error}`);
    }

    batchesCount++;

    if (batchesCount < maxBatches) {
      await sleep(100);
    }
  }

  log('Reached maximum batches count');

  // Cleanup
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
  const args = parseArgs();

  try {
    log('Sink Data Producer Example');
    const client = await createClient(args.address);
    await produceData(client, args.stream, args.topic);
    await client.destroy();
  } catch (error) {
    log(`Error: ${error}`);
    process.exitCode = 1;
  }
}

await main();
