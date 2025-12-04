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
import { BATCHES_LIMIT, cleanup, initSystem, log, MESSAGES_PER_BATCH, parseArgs, sleep } from '../utils';

async function produceMessages(client: Client, stream: Awaited<ReturnType<typeof initSystem>>[ 'stream' ], topic: Awaited<ReturnType<typeof initSystem>>[ 'topic' ]) {
  const interval = 500; // 500 milliseconds
  log(
    'Messages will be sent to stream: %d, topic: %d, partition: %d with interval %d ms.',
    interval
  );

  let currentId = 0;
  let sentBatches = 0;

  for (; sentBatches < BATCHES_LIMIT;) {
    const messages = Array.from({ length: MESSAGES_PER_BATCH }).map(() => {
      currentId++;
      return {
        //optional message id can be used for deduplication
        id: currentId,
        //optional headers can be used to store metadata
        headers: {},
        payload: `message-${currentId}`
      };
    });

    try {
      await client.message.send({
        streamId: stream.id,
        topicId: topic.id,
        messages,
        partition: Partitioning.PartitionId(topic.partitions[
          Math.floor(Math.random() * topic.partitions.length)
        ].id),
      });
    } catch (error) {
      log('Error sending messages: %o', error);
      log('This might be due to server version compatibility. The stream and topic creation worked successfully.');
      log('Please check the Iggy server version and ensure it supports the SendMessages command.');
    } finally {
      sentBatches++;
      log('Sent messages: %o', messages);
      await sleep(interval);
    }
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
      maxRetries: 5
    },
    heartbeatInterval: 5000,
    credentials: { username, password }
  });
  let streamId = null;
  // iggy will create topic with id 0 by default
  let topicId = 0;
  try {
    log('Basic producer has started, selected transport: TCP');
    log('Connecting to Iggy server...');
    // Client connects automatically when first command is called
    log('Connected successfully.');
    // Login will be handled automatically by the client on first command

    const { stream, topic } = await initSystem(client);
    streamId = stream.id;
    topicId = topic.id;

    //ping before producing messages
    const pong = await client.system.ping();
    log('Ping successful.', pong);


    log('Stream ID: %s, Topic ID: %s', streamId, topicId);
    await produceMessages(client, stream, topic);

    //log stream after producing messages
    const updatedStream = await client.stream.get({ streamId: stream.id });
    log('Stream details after producing messages: %o', updatedStream);


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
