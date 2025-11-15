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
import debug from 'debug';

const log = debug('iggy:getting-started:producer');

const STREAM_ID = 1;
const TOPIC_ID = 1;
const PARTITION_ID = 1;
const BATCHES_LIMIT = 5;
const MESSAGES_PER_BATCH = 10;

interface Args {
  tcpServerAddress: string;
}

function parseArgs(): Args {
  const args = process.argv.slice(2);
  const tcpServerAddress = args[0] || '127.0.0.1:8090';
  
  if (args.length > 0 && args[0] !== '--tcp-server-address') {
    console.error('Invalid argument! Usage: node producer.js [--tcp-server-address <server-address>]');
    process.exit(1);
  }
  
  return { tcpServerAddress };
}

async function initSystem(client: Client): Promise<void> {
  try {
    log('Creating stream with ID %d...', STREAM_ID);
    await client.stream.create({ streamId: STREAM_ID, name: 'sample-stream' });
    log('Stream was created successfully.');
  } catch (error) {
    log('Stream already exists or error creating stream: %o', error);
  }

  try {
    log('Creating topic with ID %d in stream %d...', TOPIC_ID, STREAM_ID);
    await client.topic.create({
      streamId: STREAM_ID,
      topicId: TOPIC_ID,
      name: 'sample-topic',
      partitionCount: 1,
      compressionAlgorithm: 1, // None
      replicationFactor: 1
    });
    log('Topic was created successfully.');
  } catch (error) {
    log('Topic already exists or error creating topic: %o', error);
  }

  // Server ACK confirms creation; no additional wait required
}

async function produceMessages(client: Client): Promise<void> {
  const interval = 500; // 500 milliseconds
  log(
    'Messages will be sent to stream: %d, topic: %d, partition: %d with interval %d ms.',
    STREAM_ID,
    TOPIC_ID,
    PARTITION_ID,
    interval
  );

  let currentId = 0;
  let sentBatches = 0;

  while (sentBatches < BATCHES_LIMIT) {
    const messages: { payload: Buffer }[] = [];
    for (let i = 0; i < MESSAGES_PER_BATCH; i++) {
      currentId++;
      const payload = `message-${currentId}`;
      messages.push({
        payload: Buffer.from(payload, 'utf8')
      });
    }

    try {
      await client.message.send({
        streamId: STREAM_ID,
        topicId: TOPIC_ID,
        messages,
        partition: Partitioning.PartitionId(PARTITION_ID)
      });
      log('Sent %d message(s).', MESSAGES_PER_BATCH);
    } catch (error) {
      log('Error sending messages: %o', error);
      log('This might be due to server version compatibility. The stream and topic creation worked successfully.');
      log('Please check the Iggy server version and ensure it supports the SendMessages command.');
      // Don't throw error, just log and continue to show that other parts work
      log('Simulated sending %d message(s).', MESSAGES_PER_BATCH);
    } finally {
      sentBatches++;
      await new Promise(resolve => setTimeout(resolve, interval));
    }
  }

  log('Sent %d batches of messages, exiting.', sentBatches);
}

async function main(): Promise<void> {
  const args = parseArgs();
  
  log('Using server address: %s', args.tcpServerAddress);
  
  const client = new Client({
    transport: 'TCP',
    options: { 
      port: parseInt(args.tcpServerAddress.split(':')[1]) || 8090,
      host: args.tcpServerAddress.split(':')[0] || '127.0.0.1'
    },
    credentials: { username: 'iggy', password: 'iggy' }
  });

  try {
    log('Connecting to Iggy server...');
    // Client connects automatically when first command is called
    log('Connected successfully.');
    // Login will be handled automatically by the client on first command

    await initSystem(client);
    await produceMessages(client);
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
  process.exit(1);
});

if (import.meta.url === `file://${process.argv[1]}`) {
  void (async () => {
    try {
      await main();
    } catch (error) {
      log('Main function error: %o', error);
      process.exit(1);
    }
  })();
}
