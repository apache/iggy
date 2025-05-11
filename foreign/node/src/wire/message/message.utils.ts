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


import Debug from 'debug';
import { uint32ToBuf } from '../number.utils.js';
import { serializeHeaders, type Headers } from './header.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { serializePartitioning, type Partitioning } from './partitioning.utils.js';
import { parse as parseUUID } from '../uuid.utils.js';

const debug = Debug('iggy:client');

export type MessageIdKind = 0 | 0n | string;

export type CreateMessage = {
  id?: MessageIdKind, 
  headers?: Headers,
  payload: string | Buffer
};

export const isValidMessageId = (x?: unknown): x is MessageIdKind =>
  x === undefined || x === 0 || x === 0n || 'string' === typeof x;

export const serializeMessageId = (id?: unknown) => {

  if(!isValidMessageId(id))
    throw new Error(`invalid message id: '${id}' (use uuid string or 0)`)

  if(id === undefined || id === 0 || id === 0n) {
    return Buffer.alloc(16, 0);
  }

  try {
    const uuid = parseUUID(id);
    return Buffer.from(uuid.toHex(), 'hex');
  } catch (err) {
    throw new Error(`invalid message id: '${id}' (use uuid string or 0)`, { cause: err })
  }
}

export const serializeMessage = (msg: CreateMessage) => {
  const { id, headers, payload } = msg;

  const bId = serializeMessageId(id);
  const bHeaders = serializeHeaders(headers);
  const bHLen = uint32ToBuf(bHeaders.length);
  const bPayload = 'string' === typeof payload ? Buffer.from(payload) : payload
  const bPLen = uint32ToBuf(bPayload.length);
  
  const r = Buffer.concat([
    bId,
    bHLen,
    bHeaders,
    bPLen,
    bPayload
  ]);

  debug(
    'id', bId.length, bId.toString('hex'),
    // 'binLength/HD', bHLen.length, bHLen.toString('hex'),
    'headers', bHeaders.length, bHeaders.toString('hex'),
    'binLength/PL', bPLen.length, bPLen.toString('hex'),
    'payload', bPayload.length, bPayload.toString('hex'),
    'full len', r.length //, r.toString('hex')
  );
  
  return r;
};

export const serializeMessages = (messages: CreateMessage[]) =>
  Buffer.concat(messages.map(c => serializeMessage(c)));

export const serializeSendMessages = (
  streamId: Id,
  topicId: Id,
  messages: CreateMessage[],
  partitioning?: Partitioning,
) => {
  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const bPartitioning = serializePartitioning(partitioning);
  const bMessages = serializeMessages(messages);

  return Buffer.concat([
    streamIdentifier,
    topicIdentifier,
    bPartitioning,
    bMessages
  ]);
};
