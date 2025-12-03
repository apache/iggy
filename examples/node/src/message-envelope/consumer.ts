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
import { initSystem, cleanup, log, BATCHES_LIMIT, MESSAGES_PER_BATCH } from '../utils';


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

interface MessageEnvelope {
  message_type: string;
  payload: string;
}

const ORDER_CREATED_TYPE = 'OrderCreated';
const ORDER_CONFIRMED_TYPE = 'OrderConfirmed';
const ORDER_REJECTED_TYPE = 'OrderRejected';

function parseArgs() {
  const args = process.argv.slice(2);
  const connectionString = args[0] || 'iggy+tcp://iggy:iggy@127.0.0.1:8090';

  if (args.length > 0 && (args[0] === '-h' || args[0] === '--help')) {
    log('Usage: node consumer.ts [connection_string]');
    log('Example: node consumer.ts iggy+tcp://iggy:iggy@127.0.0.1:8090');
    process.exit(0);
  }

  return { connectionString };
}



function handleMessage(payload: string): void {
  // The payload can be of any type as it is a raw byte array. In this case it's a JSON string.
  const json = payload;
  // The message envelope can be used to send the different types of messages to the same topic.
  const envelope: MessageEnvelope = JSON.parse(json);
  log('Handling message type: %s...', envelope.message_type);

  switch (envelope.message_type) {
    case ORDER_CREATED_TYPE: {
      const orderCreated: OrderCreated = JSON.parse(envelope.payload);
      log('Order Created: %o', orderCreated);
      break;
    }
    case ORDER_CONFIRMED_TYPE: {
      const orderConfirmed: OrderConfirmed = JSON.parse(envelope.payload);
      log('Order Confirmed: %o', orderConfirmed);
      break;
    }
    case ORDER_REJECTED_TYPE: {
      const orderRejected: OrderRejected = JSON.parse(envelope.payload);
      log('Order Rejected: %o', orderRejected);
      break;
    }
    default: {
      log('Received unknown message type: %s', envelope.message_type);
    }
  }
}

async function consumeMessages(
  client: Client,
  stream: Awaited<ReturnType<typeof initSystem>>['stream'],
  topic: Awaited<ReturnType<typeof initSystem>>['topic']
) {
  const interval = 500; // 500 milliseconds
  log(
    'Messages will be consumed from stream: %d, topic: %d, partition: %d with interval %d ms.',
    stream.id,
    topic.id,
    topic.partitions[0].id,
    interval
  );

  let offset = 0;
  let consumedBatches = 0;

  while (consumedBatches < BATCHES_LIMIT) {
    try {
      log('Polling for messages...');
      const polledMessages = await client.message.poll({
        streamId: stream.id,
        topicId: topic.id,
        consumer: Consumer.Single,
        partitionId: topic.partitions[0].id,
        pollingStrategy: PollingStrategy.Offset(BigInt(offset)),
        count: MESSAGES_PER_BATCH,
        autocommit: false,
      });

      if (!polledMessages || polledMessages.messages.length === 0) {
        log('No messages available.');
        consumedBatches++;
        await new Promise(resolve => setTimeout(resolve, interval));
        continue;
      }

      offset += polledMessages.messages.length;

      for (const message of polledMessages.messages) {
        const payload = new TextDecoder().decode(new Uint8Array(Object.values(message.payload)));
        handleMessage(payload);
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
    log('Message envelope consumer has started, selected transport: TCP');
    log('Connecting to Iggy server...');
    // Client connects automatically when first command is called
    log('Connected successfully.');
    // Login will be handled automatically by the client on first command

    const { stream, topic } = await initSystem(client);
    streamId = stream.id;
    topicId = topic.id;
    await consumeMessages(client, stream, topic);
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

await main();
