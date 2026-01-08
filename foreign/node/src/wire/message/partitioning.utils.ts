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


import { uint32ToBuf, uint64ToBuf } from '../number.utils.js';
import type { ValueOf } from '../../type.utils.js';


/**
 * Enumeration of partition selection strategies.
 */
export const PartitionKind = {
  /** Server selects partition using round-robin */
  Balanced : 1,
  /** Client specifies exact partition ID */
  PartitionId : 2,
  /** Client provides a key for consistent hashing */
  MessageKey : 3
} as const;

/** Type alias for the PartitionKind object */
export type PartitionKind = typeof PartitionKind;
/** String literal type of partition kind names */
export type PartitionKindId = keyof PartitionKind;
/** Numeric values of partition kinds */
export type PartitionKindValue = ValueOf<PartitionKind>

/** Balanced partitioning (server selects partition) */
export type Balanced = {
  kind: PartitionKind['Balanced'],
  value: null
};

/** Explicit partition ID selection */
export type PartitionId = {
  kind: PartitionKind['PartitionId'],
  /** Partition ID (uint32) */
  value: number
};

/** Possible types for message key values */
export type MessageKeyValue = string | number | bigint | Buffer;

/** Message key-based partitioning for consistent hashing */
export type MessageKey = {
  kind: PartitionKind['MessageKey'],
  value: MessageKeyValue
};

/** Union of all partitioning strategies */
export type Partitioning = Balanced | PartitionId | MessageKey;

/** Balanced partitioning constant */
const Balanced: Balanced = {
  kind: PartitionKind.Balanced,
  value: null
};

/**
 * Creates a partition ID partitioning strategy.
 *
 * @param id - Partition ID to target
 * @returns PartitionId partitioning object
 */
const PartitionId = (id: number): PartitionId => ({
  kind: PartitionKind.PartitionId,
  value: id
});

/**
 * Creates a message key partitioning strategy.
 *
 * @param key - Key for consistent hashing
 * @returns MessageKey partitioning object
 */
const MessageKey = (key: MessageKeyValue): MessageKey => ({
  kind: PartitionKind.MessageKey,
  value: key
});

/**
 * Factory object for creating partitioning strategies.
 */
export const Partitioning = {
  Balanced,
  PartitionId,
  MessageKey
};

/**
 * Serializes a message key value to a buffer.
 *
 * @param v - Message key value
 * @returns Serialized buffer
 * @throws Error if the value type is not supported
 */
export const serializeMessageKey = (v: MessageKeyValue) => {
  if (v instanceof Buffer) return v;
  if ('string' === typeof v) return Buffer.from(v);
  if ('number' === typeof v) return uint32ToBuf(v);
  if ('bigint' === typeof v) return uint64ToBuf(v);
  throw new Error(`cannot serialize messageKey ${v}, ${typeof v}`);
};

/**
 * Serializes the value portion of a partitioning strategy.
 *
 * @param part - Partitioning strategy
 * @returns Serialized value buffer
 */
export const serializePartitioningValue = (part: Partitioning): Buffer => {
  const { kind, value } = part;
  switch (kind) {
    case PartitionKind.Balanced: return Buffer.alloc(0);
    case PartitionKind.PartitionId: return uint32ToBuf(value);
    case PartitionKind.MessageKey: return serializeMessageKey(value);
  }
};

/** Default partitioning strategy (balanced) */
export const default_partionning: Balanced = {
  kind: PartitionKind.Balanced,
  value: null
};

/**
 * Serializes a partitioning strategy to wire format.
 * Format: [kind (1 byte)][value_length (1 byte)][value]
 *
 * @param p - Optional partitioning strategy (defaults to balanced)
 * @returns Serialized partitioning buffer
 */
export const serializePartitioning = (p?: Partitioning) => {
  const part = p || default_partionning;
  const b = Buffer.alloc(2);
  const bValue = serializePartitioningValue(part);
  b.writeUint8(part.kind);
  b.writeUint8(bValue.length, 1);
  return Buffer.concat([
    b,
    bValue
  ]);
};

