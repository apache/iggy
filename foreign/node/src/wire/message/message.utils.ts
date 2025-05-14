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
import { serializeIggyMessageHeader } from './iggy-header.utils.js';

const debug = Debug('iggy:client');

/** index size per messages in bit */
const INDEX_SIZE = 16;

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
    return Buffer.alloc(16, 0); // 0u128
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
  const bUserHeaders = serializeHeaders(headers);
  const bPayload = 'string' === typeof payload ? Buffer.from(payload) : payload
  const bIggyMessageHeader = serializeIggyMessageHeader(bId, bPayload, bUserHeaders);
  
  const r = Buffer.concat([
    bIggyMessageHeader,
    bPayload,
    bUserHeaders
  ]);

  debug(
    'id', bId.length, bId.toString('hex'),
    'iggyHeaders', bIggyMessageHeader.length, bIggyMessageHeader.toString('hex'),
    'userHeaders', bUserHeaders.length, bUserHeaders.toString('hex'),
    'payload', bPayload.length, bPayload.toString('hex'),
    'full len', r.length //, r.toString('hex')
  );
  
  return r;
};

export const serializeMessages = (messages: CreateMessage[]) =>
  messages.map(c => serializeMessage(c));

export const createMessagesIndex = (messages: Buffer[]) => {
  const bIndex = Buffer.allocUnsafe(messages.length * INDEX_SIZE);
  let currentIndex = 0;
  let msgsSize = 0;
  messages.forEach(msg => {
    msgsSize += msg.length;
    bIndex.writeBigUInt64LE(0n, currentIndex)
    bIndex.writeUInt32LE(msgsSize, currentIndex + 4)
    bIndex.writeBigUInt64LE(0n, currentIndex + 8)
    currentIndex += INDEX_SIZE;
  });
  return bIndex;
}

export const serializeSendMessages = (
  streamId: Id,
  topicId: Id,
  messages: CreateMessage[],
  partitioning?: Partitioning,
) => {
  const streamIdentifier = serializeIdentifier(streamId);
  const topicIdentifier = serializeIdentifier(topicId);
  const bPartitioning = serializePartitioning(partitioning);
  const bMessagesCount = uint32ToBuf(messages.length);
  const bMetadataLen = uint32ToBuf(
    streamIdentifier.length + topicIdentifier.length +
      bPartitioning.length + bMessagesCount.length
  );

  const bMessagesArray = serializeMessages(messages);
  const bMessageIndex = createMessagesIndex(bMessagesArray);

  return Buffer.concat([
    bMetadataLen,
    streamIdentifier,
    topicIdentifier,
    bPartitioning,
    bMessagesCount,
    bMessageIndex,
    ...bMessagesArray
  ]);
};
