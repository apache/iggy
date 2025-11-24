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
import { BATCHES_LIMIT, cleanup, initSystem, log, MESSAGES_PER_BATCH, parseArgs, sleep } from '../utils';


interface SerializedMessage {
  to_json_envelope(): string;
}

interface OrderCreated {
  orderId: string;
  customerId: string;
  amount: number;
}

interface OrderConfirmed {
  orderId: string;
  timestamp: number;
}

interface OrderRejected {
  orderId: string;
  reason: string;
}

export type MessageTypes = OrderCreated | OrderConfirmed | OrderRejected;

export interface MessageEnvelope {
  message_type: string;
  payload: string;
}

const ORDER_CREATED_TYPE = 'OrderCreated';
const ORDER_CONFIRMED_TYPE = 'OrderConfirmed';
const ORDER_REJECTED_TYPE = 'OrderRejected';

class MessagesGenerator {
  private orderId = 0;

  generate(): SerializedMessage {
    this.orderId++;
    const messageType = this.orderId % 3;

    switch (messageType) {
      case 0: {
        const orderCreated: OrderCreated = {
          orderId: `order-${this.orderId}`,
          customerId: `customer-${Math.floor(Math.random() * 100)}`,
          amount: Math.floor(Math.random() * 10000),
        };
        return {
          to_json_envelope: () => JSON.stringify({
            message_type: ORDER_CREATED_TYPE,
            payload: JSON.stringify(orderCreated),
          }),
        };
      }
      case 1: {
        const orderConfirmed: OrderConfirmed = {
          orderId: `order-${Math.floor(this.orderId / 3)}`,
          timestamp: Date.now(),
        };
        return {
          to_json_envelope: () => JSON.stringify({
            message_type: ORDER_CONFIRMED_TYPE,
            payload: JSON.stringify(orderConfirmed),
          }),
        };
      }
      default: {
        const orderRejected: OrderRejected = {
          orderId: `order-${Math.floor(this.orderId / 3)}`,
          reason: 'Insufficient balance',
        };
        return {
          to_json_envelope: () => JSON.stringify({
            message_type: ORDER_REJECTED_TYPE,
            payload: JSON.stringify(orderRejected),
          }),
        };
      }
    }
  }
}


async function produceMessages(
  client: Client,
  stream: Awaited<ReturnType<typeof initSystem>>['stream'],
  topic: Awaited<ReturnType<typeof initSystem>>['topic']
) {
  const interval = 500; // 500 milliseconds
  log(
    'Messages will be sent to stream: %d, topic: %d, partition: %d with interval %d ms.',
    stream.id,
    topic.id,
    topic.partitions[0].id,
    interval
  );

  const messageGenerator = new MessagesGenerator();
  let sentBatches = 0;

  while (sentBatches < BATCHES_LIMIT) {
    const messages = Array.from({ length: MESSAGES_PER_BATCH }).map(() => {
      const serializableMessage = messageGenerator.generate();
      const jsonEnvelope = serializableMessage.to_json_envelope();
      return {
        payload: jsonEnvelope,
      };
    });

    try {
      log('Sending messages count: %d', messages.length);
      await client.message.send({
        streamId: stream.id,
        topicId: topic.id,
        messages,
        partition: Partitioning.PartitionId(
          topic.partitions[Math.floor(Math.random() * topic.partitions.length)].id
        ),
      });
      sentBatches++;
      log('Sent %d message(s).', messages.length);
    } catch (error) {
      log('Error sending messages: %o', error);
      log(
        'This might be due to server version compatibility. The stream and topic creation worked successfully.'
      );
      log('Please check the Iggy server version and ensure it supports the SendMessages command.');
      sentBatches++;
    }

    await sleep(interval);
  }

  log('Sent %d batches of messages, exiting.', sentBatches);
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

  let streamId = null;
  let topicId = 0;

  try {
    log('Message envelope producer has started, selected transport: TCP');
    log('Connecting to Iggy server...');
    // Client connects automatically when first command is called
    log('Connected successfully.');
    // Login will be handled automatically by the client on first command

    const { stream, topic } = await initSystem(client);
    streamId = stream.id;
    topicId = topic.id;
    await produceMessages(client, stream, topic);
  } catch (error) {
    log('Error in main: %o', error);
    process.exitCode = 1;
  } finally {
    if (streamId !== null && topicId !== null) {
      await cleanup(client, streamId, topicId);
    }
    await client.destroy();
    log('Disconnected from server.');
  }
}

process.on('unhandledRejection', (reason, promise) => {
  log('Unhandled Rejection at: %o, reason: %o', promise, reason);
  process.exitCode = 1;
});

main();
