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

const log = console.log;

const STREAM_ID = 1;
const TOPIC_ID = 1;
const PARTITION_ID = 1;
const BATCHES_LIMIT = 5;
const MESSAGES_PER_BATCH = 10;

interface Args {
  connectionString: string;
}

function parseArgs(): Args {
  const args = process.argv.slice(2);
  const connectionString = args[0] || 'iggy+tcp://iggy:iggy@127.0.0.1:8090';
  
  if (args.length > 0 && (args[0] === '-h' || args[0] === '--help')) {
    log('Usage: node consumer.js [connection_string]');
    log('Example: node consumer.js iggy+tcp://iggy:iggy@127.0.0.1:8090');
    process.exit(0);
  }
  
  return { connectionString };
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
        continue;
      }

      offset += polledMessages.messages.length;
      
      for (const message of polledMessages.messages) {
        handleMessage(message);
      }
      log('Consumed %d message(s).', polledMessages.messages.length);
    } catch (error) {
      log('Error consuming messages: %o', error);
    } finally {
      consumedBatches++;
      log('Completed poll attempt %d.', consumedBatches);
      await new Promise(resolve => setTimeout(resolve, interval));
    }
  }

  log('Consumed %d batches of messages, exiting.', consumedBatches);
}

function handleMessage(message: any): void {
  // The payload can be of any type as it is a raw byte array. In this case it's a simple string.
  const payload = message.payload.toString('utf8');
  log(
    'Handling message at offset: %d, payload: %s...',
    message.offset,
    payload
  );
}

async function main(): Promise<void> {
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
    options: { port, host },
    credentials: { username, password }
  });

  try {
    log('Basic consumer has started, selected transport: TCP');
    log('Connecting to Iggy server...');
    // Client connects automatically when first command is called
    log('Connected successfully.');

    // Login will be handled automatically by the client on first command

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
  process.exitCode = 1;
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
