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


import { PollingStrategy, singleConsumerStream, Consumer } from '../index.js';
import { open } from 'node:fs/promises';
import { resolve } from 'node:path';
import { getClient } from './utils.js';
import type { PollMessagesResponse } from '../wire/index.js';


export const topicToFile = async (
  filepath: string,
  streamName: string,
  topicName: string,
  partitionId = 0
) => {
  const cli = getClient();
  const fd = await open(filepath, 'w+');

  const t = await cli.topic.get({ streamId: streamName, topicId: topicName });
  console.log('TOPIC/GET', t);

  // reset consumer offset
  try {
    const offset = await cli.offset.get({
      streamId: streamName, topicId: topicName, partitionId, consumer: Consumer.Single
    });
    if (offset)
      await cli.offset.delete({
        streamId: streamName, topicId: topicName, partitionId, consumer: Consumer.Single
      });
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
  } catch (err) {}

  const dStart = Date.now();
  const consumer = singleConsumerStream(cli._config);
  const stream = await consumer({
    streamId: streamName,
    topicId: topicName,
    partitionId,
    pollingStrategy: PollingStrategy.Next,
    count: 1,
    interval: 1000,
    endOnLastOffset: true
  });

  stream.on('data', async (polled: PollMessagesResponse) => {
    const { messages } = polled;
    return await fd.write(Buffer.concat(messages.map(m => m.payload)));
  });

  stream.once('end', () => {
    console.log('pollStream#end', `took ${Date.now() - dStart}ms`);
    fd.datasync();
    fd.close();
    stream.destroy();
  });

  stream.on('error', (err) => {
    console.log('pollStream#err', err);
    stream.destroy();
  });

  return stream;
};


const argz = process.argv.slice(2);
const [rPath, streamIdStr, topicIdStr] = argz;

if (argz.length < 3 || ['-h', '--help', '?'].includes(argz[0])) {
  console.log(`Usage: node stream-file-to-topic.js filePath streamId topicId`)
  console.log('got', argz);
  console.log('note: this script only accept numerical stream/topic id');
  process.exit(1);
}

const filepath = resolve(rPath);
const streamId = streamIdStr;
const topicId = topicIdStr;

console.log('running with params:', { filepath, streamId, topicId });

const s = await topicToFile(filepath, streamId, topicId);

s.on('end', () => {
  console.log('end, exiting');
  process.exit(0);
});

s.on('error', (err) => {
  console.error('FAILED WITH ERROR', err, 'exiting...');
  process.exit(1);
});
