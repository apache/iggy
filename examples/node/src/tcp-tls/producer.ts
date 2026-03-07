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

// TCP/TLS Producer Example
//
// Demonstrates producing messages over a TLS-encrypted TCP connection
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
//   DEBUG=iggy:* npx tsx src/tcp-tls/producer.ts

import { readFileSync } from 'node:fs';
import { Client, Partitioning } from 'apache-iggy';
import { BATCHES_LIMIT, cleanup, initSystem, log, MESSAGES_PER_BATCH, sleep } from '../utils';

async function produceMessages(
  client: Client,
  streamId: number | string,
  topicId: number | string,
) {
  const interval = 500;
  log(
    'Messages will be sent to stream: %s, topic: %s with interval %d ms.',
    streamId,
    topicId,
    interval,
  );

  let currentId = 0;
  let sentBatches = 0;

  for (; sentBatches < BATCHES_LIMIT;) {
    const messages = Array.from({ length: MESSAGES_PER_BATCH }).map(() => {
      currentId++;
      return {
        id: currentId,
        headers: [],
        payload: `message-${currentId}`,
      };
    });

    try {
      await client.message.send({
        streamId,
        topicId,
        messages,
        partition: Partitioning.Balanced,
      });
    } catch (error) {
      log('Error sending messages: %o', error);
    } finally {
      sentBatches++;
      log('Sent messages: %o', messages);
      await sleep(interval);
    }
  }

  log('Sent %d batches of messages, exiting.', sentBatches);
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

  let streamId: number | null = null;
  let topicId = 0;
  try {
    log('TLS producer has started, selected transport: TLS');
    log('Connecting to Iggy server over TLS...');

    const { stream, topic } = await initSystem(client);
    streamId = stream.id;
    topicId = topic.id;

    const pong = await client.system.ping();
    log('Ping successful.', pong);

    log('Stream ID: %s, Topic ID: %s', streamId, topicId);
    await produceMessages(client, streamId, topicId);
  } catch (error) {
    log('Error in main: %o', error);
    process.exitCode = 1;
  } finally {
    if (streamId) {
      await cleanup(client, streamId, topicId);
    }
    await client.destroy();
    log('Disconnected from server.');
  }
}

await main();
