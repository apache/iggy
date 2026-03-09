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

// TCP/TLS Consumer Example
//
// Demonstrates consuming messages over a TLS-encrypted TCP connection
// using custom certificates from core/certs/.
//
// Prerequisites:
//   Start the Iggy server with TLS enabled:
//     IGGY_TCP_TLS_ENABLED=true \
//     IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
//     IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
//     cargo r --bin iggy-server
//
// Run this example (from examples/node/):
//   DEBUG=iggy:* npx tsx src/tcp-tls/consumer.ts

import { readFileSync } from 'node:fs';
import { Client, PollingStrategy, Consumer } from 'apache-iggy';
import { log, PARTITION_ID, STREAM_ID, TOPIC_ID } from '../utils';

const BATCHES_LIMIT = 5;
const MESSAGES_PER_BATCH = 10;

async function consumeMessages(
  client: Client,
  streamId: number | string,
  topicId: number | string,
  partitionId: number,
) {
  const interval = 500;
  log(
    'Messages will be consumed from stream: %s, topic: %s, partition: %d with interval %d ms.',
    streamId,
    topicId,
    partitionId,
    interval,
  );

  let offset = 0;
  let consumedBatches = 0;

  while (consumedBatches < BATCHES_LIMIT) {
    try {
      log('Polling for messages...');
      const polledMessages = await client.message.poll({
        streamId,
        topicId,
        consumer: Consumer.Single,
        partitionId,
        pollingStrategy: PollingStrategy.Offset(BigInt(offset)),
        count: MESSAGES_PER_BATCH,
        autocommit: false,
      });

      if (!polledMessages || polledMessages.messages.length === 0) {
        log('No messages found.');
        consumedBatches++;
        await new Promise(resolve => setTimeout(resolve, interval));
        continue;
      }

      offset += polledMessages.messages.length;

      for (const message of polledMessages.messages) {
        const payload = message.payload.toString('utf8');
        const { offset: msgOffset, timestamp } = message.headers;
        log('Received message: %s (offset: %d, timestamp: %d)', payload, msgOffset, timestamp);
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

async function main() {
  // Configure the client with TLS transport.
  // transport: 'TLS'  activates TLS on the TCP connection
  // ca: readFileSync(...)  provides the CA certificate to verify the server cert
  // host: 'localhost'  must match the server certificate CN/SAN
  const client = new Client({
    transport: 'TLS',
    options: {
      port: 8090,
      host: 'localhost',
      ca: readFileSync('../../core/certs/iggy_ca_cert.pem'),
    },
    credentials: { username: 'iggy', password: 'iggy' },
  });

  try {
    log('TLS consumer has started, selected transport: TLS');
    log('Connecting to Iggy server over TLS...');

    await consumeMessages(client, STREAM_ID, TOPIC_ID, PARTITION_ID);
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

main();
