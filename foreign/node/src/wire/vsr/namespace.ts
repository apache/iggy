// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//

/**
 * Namespace packing and the partition-plane payload peeks needed to derive
 * it, ported from `core/binary_protocol/src/namespace.rs` and
 * `namespace_for_request` in `core/sdk/src/vsr.rs`.
 */

import { COMMAND_CODE } from '../command.code.js';
import { responseError } from '../error.utils.js';
import { Operation, isMetadata } from './operation.js';

/** `IggyError::InvalidCommand`. */
const INVALID_COMMAND = 3;
/** `IggyError::FeatureUnavailable`. */
const FEATURE_UNAVAILABLE = 5;
/** `IggyError::InvalidIdentifier`. */
const INVALID_IDENTIFIER = 6;

const MAX_STREAMS = 4096;
const MAX_TOPICS = 4096;
const MAX_PARTITIONS = 1_000_000;
const STREAM_SHIFT = 32n;
const TOPIC_SHIFT = 20n;

/**
 * Control-plane requests target the metadata replica (shard 0), selected by
 * this exact sentinel. Plain 0 would fall into namespace hashing and land a
 * Register on a peer shard.
 */
export const METADATA_CONSENSUS_NAMESPACE = 1n << 63n;

/** Packs stream / topic / partition ids into a routing namespace. */
export const packNamespace = (
  streamId: number,
  topicId: number,
  partitionId: number
): bigint => {
  validateField(streamId, MAX_STREAMS);
  validateField(topicId, MAX_TOPICS);
  validateField(partitionId, MAX_PARTITIONS);
  return (BigInt(streamId) << STREAM_SHIFT) |
    (BigInt(topicId) << TOPIC_SHIFT) |
    BigInt(partitionId);
};

/**
 * Selects the routing namespace for a request. Partition-plane commands
 * derive it from their own payload; a named stream or topic identifier
 * yields 0 so the server resolves the name.
 *
 * @throws Error mirroring the Rust SDK: invalid-identifier for an
 *   out-of-range field, invalid-command for an undecodable payload, and
 *   feature-unavailable for a partition operation this SDK cannot derive.
 */
export const namespaceForRequest = (
  code: number,
  payload: Buffer,
  operation: number
): bigint => {
  if (operation === Operation.Register || operation === Operation.Logout)
    return METADATA_CONSENSUS_NAMESPACE;
  if (operation === Operation.NonReplicated || isMetadata(operation))
    return 0n;

  switch (code) {
    case COMMAND_CODE.SendMessages:
      return namespaceFromSendMessages(payload);
    case COMMAND_CODE.StoreOffset:
    case COMMAND_CODE.DeleteConsumerOffset:
    case COMMAND_CODE.StoreOffset2:
    case COMMAND_CODE.DeleteConsumerOffset2:
      return namespaceFromConsumerOffset(payload);
    case COMMAND_CODE.DeleteSegments:
      return namespaceFromDeleteSegments(payload);
    default:
      // The guard that keeps custom partition operations unreachable.
      throw responseError(code, FEATURE_UNAVAILABLE);
  }
};

/** A decoded identifier: numeric value, or null for a name (server resolves). */
type PeekedIdentifier = {
  numeric: number | null,
  length: number
};

const IDENTIFIER_KIND_NUMERIC = 1;
const IDENTIFIER_KIND_STRING = 2;

const peekIdentifier = (payload: Buffer, offset: number): PeekedIdentifier => {
  if (payload.length < offset + 2)
    throw responseError(0, INVALID_COMMAND);
  const kind = payload.readUInt8(offset);
  const length = payload.readUInt8(offset + 1);
  if (payload.length < offset + 2 + length)
    throw responseError(0, INVALID_COMMAND);
  if (kind === IDENTIFIER_KIND_NUMERIC) {
    if (length !== 4)
      throw responseError(0, INVALID_COMMAND);
    return { numeric: payload.readUInt32LE(offset + 2), length: 2 + length };
  }
  if (kind === IDENTIFIER_KIND_STRING && length > 0)
    return { numeric: null, length: 2 + length };
  throw responseError(0, INVALID_COMMAND);
};

const validateField = (value: number, exclusiveMax: number): void => {
  if (value >= exclusiveMax)
    throw responseError(0, INVALID_IDENTIFIER);
};

const namespaceFromIds = (
  stream: PeekedIdentifier,
  topic: PeekedIdentifier,
  partitionId: number
): bigint => {
  // Named identifiers defer resolution to the server.
  if (stream.numeric === null || topic.numeric === null) return 0n;
  return packNamespace(stream.numeric, topic.numeric, partitionId);
};

/**
 * `SendMessages`: `[metadata_len u32][stream ident][topic ident]
 * [partitioning kind u8, len u8, value]...`. Only explicit `PartitionId`
 * partitioning is routable under VSR; the broker never picks a partition.
 */
const namespaceFromSendMessages = (payload: Buffer): bigint => {
  if (payload.length < 4)
    throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
  const metadataLength = payload.readUInt32LE(0);
  if (payload.length < 4 + metadataLength)
    throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
  // Rust peeks inside payload[4..4 + metadata_length]; a read past the
  // declared metadata region must fail rather than spill into message bytes
  // and derive a namespace the server would never compute.
  const metadata = payload.subarray(4, 4 + metadataLength);

  let offset = 0;
  const stream = peekIdentifier(metadata, offset);
  offset += stream.length;
  const topic = peekIdentifier(metadata, offset);
  offset += topic.length;

  if (metadata.length < offset + 2)
    throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
  const partitioningKind = metadata.readUInt8(offset);
  const partitioningLength = metadata.readUInt8(offset + 1);
  const PARTITIONING_PARTITION_ID = 2;
  if (partitioningKind !== PARTITIONING_PARTITION_ID)
    throw responseError(COMMAND_CODE.SendMessages, FEATURE_UNAVAILABLE);
  if (partitioningLength !== 4 || metadata.length < offset + 2 + 4)
    throw responseError(COMMAND_CODE.SendMessages, INVALID_COMMAND);
  const partitionId = metadata.readUInt32LE(offset + 2);

  return namespaceFromIds(stream, topic, partitionId);
};

/**
 * Consumer-offset requests: `[consumer kind u8][consumer ident]
 * [stream ident][topic ident][partition flag u8][partition u32]...`.
 */
const namespaceFromConsumerOffset = (payload: Buffer): bigint => {
  if (payload.length < 1 || (payload.readUInt8(0) !== 1 &&
      payload.readUInt8(0) !== 2))
    throw responseError(COMMAND_CODE.StoreOffset, INVALID_COMMAND);
  let offset = 1;
  const consumer = peekIdentifier(payload, offset);
  offset += consumer.length;
  const stream = peekIdentifier(payload, offset);
  offset += stream.length;
  const topic = peekIdentifier(payload, offset);
  offset += topic.length;

  if (payload.length < offset + 5)
    throw responseError(COMMAND_CODE.StoreOffset, INVALID_COMMAND);
  const hasPartition = payload.readUInt8(offset) === 1;
  if (!hasPartition)
    throw responseError(COMMAND_CODE.StoreOffset, INVALID_IDENTIFIER);
  const partitionId = payload.readUInt32LE(offset + 1);

  return namespaceFromIds(stream, topic, partitionId);
};

/** `DeleteSegments`: `[stream ident][topic ident][partition u32]...`. */
const namespaceFromDeleteSegments = (payload: Buffer): bigint => {
  let offset = 0;
  const stream = peekIdentifier(payload, offset);
  offset += stream.length;
  const topic = peekIdentifier(payload, offset);
  offset += topic.length;

  if (payload.length < offset + 4)
    throw responseError(COMMAND_CODE.DeleteSegments, INVALID_COMMAND);
  const partitionId = payload.readUInt32LE(offset);

  return namespaceFromIds(stream, topic, partitionId);
};
