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


import assert from 'node:assert/strict';
import { ClientConfig, SingleClient } from './client/index.js';
import { groupConsumerStream } from './stream/consumer-stream.js';
import { PollingStrategy, type PollMessagesResponse } from './wire/index.js';

const streamName = 'debug-snd-stream';
const topicName = 'debug-snd-topic';
const groupName = 'debug-snd-group';

const stream = {
  name: streamName
};

const topic = {
  streamId: streamName,
  name: topicName,
  partitionCount: 1,
  compressionAlgorithm: 1
};

const opt: ClientConfig = {
  transport: 'TCP' as const,
  options: {
    port: 8090,
    host: '127.0.0.1' ,
    // allowHalfOpen: true,
    // keepAlive: true
  },
  credentials: { username: 'iggy', password: 'iggy' },
  reconnect: {
    enabled: true,
    interval: 10 * 1000,
    maxRetries: 10
  }
};

const c = new SingleClient(opt);

const cleanup = async () => {
  assert.ok(await c.stream.delete({ streamId: streamName }));
  assert.ok(await c.session.logout());
  c.destroy();
}

const msg = {
  streamId: streamName,
  topicId: topicName,
  messages: [
    { payload: 'yolo msg 2' },
    { payload: 'yolo msg v4' },
    { payload: 'yolo msg v7' },
  ],
};

try {

  await c.stream.create(stream);
  console.log('server stream CREATED::', { streamName });
  await c.topic.create(topic);
  console.log('server topic CREATED::', { topicName });

  // send
  setInterval(async () => {
    assert.ok(await c.message.send(msg));
    console.log('message SEND::', { msg })
  }, 3000);


  // POLL MESSAGE
  const pollReq = {
    groupName,
    streamId: streamName,
    topicId: topicName,
    pollingStrategy: PollingStrategy.Next,
    count: 1,
    interval: 5000
  };

  const cs = await groupConsumerStream(opt)(pollReq);
  const dataCb = (d: PollMessagesResponse) => {
    console.log('cli/DATA POLLED:', d);
    // recv += d.count;
    // if (recv === ct)
    //   str.destroy();
  };

  cs.on('data', dataCb);

  cs.on('error', (err) => {
    console.error('cli/=>>Stream ERROR:: // DESTROY ', err)
    cs.destroy(err);
  });

  cs.on('end', async () => {
    console.log('cli/=>>Stream END::')
    cs.destroy();
    await cleanup();
  });

  cs.on('close', async () => {
    console.log('cli/=>>Stream CLOSE::')
    // await cleanup();
  });


} catch (err) {
  console.error('client catchALL END', err)
  await cleanup();
}

// process.on('SIGINT', async () => {
//   await cleanup();
//   console.log('cleanup OK, exiting...');
//   process.exit()
// });
