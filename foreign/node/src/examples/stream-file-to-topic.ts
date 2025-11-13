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


import { open } from 'node:fs/promises';
import { resolve } from 'node:path';
import { pipeline } from 'node:stream/promises';
import { Writable, type TransformCallback } from 'node:stream';
import { HeaderValue } from '../index.js';
import { getClient } from './utils.js';

export const fileToTopic = async (
  filepath: string,
  streamName: string,
  topicName: string,
  highWaterMark = 512 * 1024
) => {
  const cli = getClient();
  await cli.stream.ensure(streamName);
  await cli.topic.ensure(streamName, topicName);
  const fd = await open(filepath);
  const fname = filepath.split('/').pop() || filepath;
  const st = await fd.stat();
  console.log('FILE/STAT', fname, '~', (st.size / (1024 * 1024)).toFixed(2), 'mb', st);
  const dStart = Date.now();

  try {
    let idx = 0;

    await pipeline(
      fd.createReadStream({highWaterMark}),

      new Writable({
        async write(chunks: Buffer, encoding: BufferEncoding, cb: TransformCallback) {
          const messages = [{
            headers: {
              fileindex: HeaderValue.Uint32(idx++),
              filename: HeaderValue.String(fname)
            },
            payload: chunks
          }];

          try {
            await cli.message.send({ streamId: streamName, topicId: topicName, messages });
            cb();
          } catch (err) {
            console.log('WRITE ERR', err, chunks);
            cb(err as Error);
          }
        }
      })
    );

    console.log(`Finished ! took ${Date.now() - dStart}ms`,);
  } catch (err) {
    console.error('Pipeline failed !', err);
    throw err;
  }
};


const argz = process.argv.slice(2);
const [rPath, streamIdStr, topicIdStr] = argz;

if (argz.length < 3 || ['-h', '--help', '?'].includes(argz[0])) {
  console.log(`Usage: node stream-file-to-topic.js filePath streamName topicName`)
  console.log('got', argz);
  console.log('note: this script only accept numerical stream/topic id');
  process.exit(1);
}

const filepath = resolve(rPath);
const streamName = streamIdStr;
const topicName = topicIdStr;

console.log('running with params:', { filepath, streamName, topicName })

try {
  await fileToTopic(filepath, streamName, topicName, 512 * 1024);
  // eslint-disable-next-line @typescript-eslint/no-unused-vars
} catch(err) {
  process.exit(1);
}

process.exit(0);
