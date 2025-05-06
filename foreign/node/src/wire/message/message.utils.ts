
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
