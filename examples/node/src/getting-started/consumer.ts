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

import { Client, PollingStrategy, Consumer } from 'apache-iggy';
import { BATCHES_LIMIT, log, MESSAGES_PER_BATCH, PARTITION_ID, STREAM_ID, TOPIC_ID } from '../utils';




interface Args {
  tcpServerAddress: string;
}

function parseArgs(): Args {
  const args = process.argv.slice(2);
  const tcpServerAddress = args[0] || '127.0.0.1:8090';

  if (args.length > 0 && args[0] !== '--tcp-server-address') {
    console.error('Invalid argument! Usage: node consumer.js [--tcp-server-address <server-address>]');
    process.exit(1);
  }

  return { tcpServerAddress };
}

async function consumeMessages(client: Client): Promise<void> {
  const interval = 500; // 500 milliseconds
  log(
    'Messages will be consumed from stream: %d, topic: %d, partition: %d with interval %d ms.',
    STREAM_ID,
    TOPIC_ID,
    PARTITION_ID,
    interval
  );

  let offset = 0;
  let consumedBatches = 0;

  while (consumedBatches < BATCHES_LIMIT) {
    try {
      log('Polling for messages...');
      const polledMessages = await client.message.poll({
        streamId: STREAM_ID,
        topicId: TOPIC_ID,
        consumer: Consumer.Single,
        partitionId: PARTITION_ID,
        pollingStrategy: PollingStrategy.Offset(BigInt(offset)),
        count: MESSAGES_PER_BATCH,
        autocommit: false
      });

      if (!polledMessages || polledMessages.messages.length === 0) {
        log('No messages found in current poll - this is expected if the producer had issues sending messages');
        consumedBatches++; // Increment even when no messages found
        log('Completed poll attempt %d (no messages).', consumedBatches);
        await new Promise(resolve => setTimeout(resolve, interval));
        continue;
      }

      offset += polledMessages.messages.length;

      for (const message of polledMessages.messages) {
        handleMessage(message);
      }

      consumedBatches++;
      log('Consumed %d message(s) in batch %d.', polledMessages.messages.length, consumedBatches);

      await new Promise(resolve => setTimeout(resolve, interval));
    } catch (error) {
      log('Error consuming messages: %o', error);
      throw error;
    }
  }

  log('Consumed %d batches of messages, exiting.', consumedBatches);
}

function handleMessage(message: any): void {
  // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
  const payload = message.payload.toString('utf8');
  log(
    `Handling message at offset: ${message.headers.offset}, payload: %s...`,
    payload
  );
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

    log('Logging in user...');
    await client.session.login({ username: 'iggy', password: 'iggy' });
    log('Logged in successfully.');

    await consumeMessages(client);
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
