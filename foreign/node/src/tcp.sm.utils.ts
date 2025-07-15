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
import { v7 } from './wire/uuid.utils.js';
import { sendMessages, type Partitioning, HeaderValue, type Message } from './wire/index.js';
import type { ClientProvider } from './client/client.type.js';
import type { Id } from './wire/identifier.utils.js';


const h0 = { 'foo': HeaderValue.Int32(42), 'bar': HeaderValue.Uint8(123) };
const h1 = { 'x-header-string-1': HeaderValue.String('incredible') };
const h2 = { 'x-header-bool': HeaderValue.Bool(false) };

const messages = [
  { payload: 'content with header', headers: h0 },
  { payload: 'content solo' },
  { payload: 'yolo msg' },
  { payload: 'yolo msg 2' },
  { payload: 'this is fuu', headers: h1 },
  { payload: 'this is bar', headers: h2 },
  { payload: 'yolo msg 3' },
  { payload: 'fuu again', headers: h1 },
  { payload: 'damnit', headers: h0 },
  { payload: 'yolo msg 4', Headers: h2 },
];

export const someMessageContent = () => messages[Math.floor(Math.random() * messages.length)]

export const generateMessages = (count = 1) => {
  return [...Array(count)].map(() => ({ id: v7(), ...someMessageContent() }));
}

export const sendSomeMessages = (s: ClientProvider) =>
  async (streamId: Id, topicId: Id, partition: Partitioning) => {
    const rSend = await sendMessages(s)({
      topicId, streamId, messages: generateMessages(100), partition
    });
    assert.ok(rSend);
    return rSend;
  };


export const formatPolledMessages = (msgs: Message[]) =>
  msgs.map(m => {
    const { headers: { id, offset, timestamp, checksum }, payload, userHeaders } = m;
    return {
      id,
      offset,
      headers: userHeaders,
      payload: payload.toString(),
      timestamp,
      checksum
    };
  });

export const getIggyAddress = (host = '127.0.0.1', port = 8090): [string, number] => {
  if (process.env.IGGY_TCP_ADDRESS) {
    const s = (process.env.IGGY_TCP_ADDRESS || '').split(':');
    [host, port] = [s[0], s[1] ? parseInt(s[1].toString(), 10) : port];
  }
  return [host, port];
}
