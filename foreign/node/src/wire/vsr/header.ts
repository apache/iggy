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

/**
 * VSR consensus header layout, ported from
 * `core/binary_protocol/src/consensus/header.rs`. Every consensus frame in
 * both directions starts with a 256-byte header. Fields are read and written
 * by byte offset, mirroring the Rust SDK, which avoids struct casts because
 * the headers lead with unaligned u128 fields.
 */

/** Size of every consensus header, both directions. */
export const HEADER_SIZE = 256;

/**
 * `RequestHeader` field offsets the client writes.
 *
 * The client wire carries no routing namespace: the server derives the
 * consensus group (plane from `operation`, partition target from the payload)
 * and stamps it into its own internal header. Everything that followed the
 * removed field therefore sits eight bytes earlier than in the pre-derivation
 * layout.
 */
export const REQUEST_OFFSET = {
  size: 48,
  command: 60,
  client: 128,
  timestamp: 160,
  request: 168,
  operation: 176,
  session: 184,
  reserved: 196
} as const;

/** `ReplyHeader` field offsets the client reads. */
export const REPLY_OFFSET = {
  size: 48,
  command: 60,
  operation: 208,
  status: 216
} as const;

/** `EvictionHeader` field offsets the client reads. */
export const EVICTION_OFFSET = {
  command: 60,
  client: 128,
  serverProtocolVersion: 144,
  serverProtocolVersionMin: 148,
  reason: 255
} as const;

/** `Command` frame discriminants a client encounters. */
export const Command = {
  Request: 5,
  Reply: 8,
  Eviction: 13
} as const;

/**
 * `EvictionReason` values, mirroring
 * `core/binary_protocol/src/consensus/header.rs`.
 */
export const EvictionReason = {
  Reserved: 0,
  NoSession: 1,
  ClientReleaseTooLow: 2,
  ClientReleaseTooHigh: 3,
  InvalidRequestOperation: 4,
  InvalidRequestBody: 5,
  InvalidRequestBodySize: 6,
  SessionTooLow: 7,
  SessionReleaseMismatch: 8,
  InvalidCredentials: 9,
  InvalidToken: 10,
  UserInactive: 11,
  SessionError: 12,
  StaleClient: 13,
  IncompatibleProtocol: 14,
  MalformedLogin: 15
} as const;

/** Fields the client writes into a request header. */
export type RequestHeaderFields = {
  /** Header + body total, bytes. */
  size: number,
  /** Ephemeral client identifier (u128). */
  client: bigint,
  /** Request id (u64): 0 for Register, else per session rules. */
  request: bigint,
  /** `Operation` discriminant. */
  operation: number,
  /** Bound session (u64), or 0n. */
  session: bigint,
  /** Command code for `NonReplicated`, placed in `reserved[0..4]`. */
  nonReplicatedCode?: number
};

const U64_MASK = 0xFFFFFFFFFFFFFFFFn;

/**
 * Encodes a 256-byte request header. Only the six fields the server reads
 * are written; the checksums stay zero, matching the Rust SDK's contract
 * with the VSR server.
 */
export const encodeRequestHeader = (fields: RequestHeaderFields): Buffer => {
  const header = Buffer.alloc(HEADER_SIZE);
  header.writeUInt32LE(fields.size, REQUEST_OFFSET.size);
  header.writeUInt8(Command.Request, REQUEST_OFFSET.command);
  // u128 client id: two little-endian u64 halves, low half first.
  header.writeBigUInt64LE(fields.client & U64_MASK, REQUEST_OFFSET.client);
  header.writeBigUInt64LE(fields.client >> 64n, REQUEST_OFFSET.client + 8);
  header.writeBigUInt64LE(fields.request, REQUEST_OFFSET.request);
  header.writeUInt8(fields.operation, REQUEST_OFFSET.operation);
  header.writeBigUInt64LE(fields.session, REQUEST_OFFSET.session);
  if (fields.nonReplicatedCode !== undefined)
    header.writeUInt32LE(fields.nonReplicatedCode, REQUEST_OFFSET.reserved);
  return header;
};

/**
 * Reads the frame discriminant. `command` sits at the same offset in every
 * consensus header, so one byte discriminates the frame type.
 */
export const peekCommand = (header: Buffer): number =>
  header.readUInt8(REPLY_OFFSET.command);

/** Reads the total frame size from a reply or eviction header. */
export const readSize = (header: Buffer): number =>
  header.readUInt32LE(REPLY_OFFSET.size);

/** Reads the pre-commit denial status; 0 means the reply committed. */
export const readStatus = (header: Buffer): number =>
  header.readUInt32LE(REPLY_OFFSET.status);

/** Reads the `Operation` discriminant from a reply header. */
export const readReplyOperation = (header: Buffer): number =>
  header.readUInt8(REPLY_OFFSET.operation);

/** Decoded eviction frame. */
export type Eviction = {
  reason: number,
  serverProtocolVersion: number,
  serverProtocolVersionMin: number
};

/** Reads the fields of an eviction header the client acts on. */
export const readEviction = (header: Buffer): Eviction => ({
  reason: header.readUInt8(EVICTION_OFFSET.reason),
  serverProtocolVersion:
    header.readUInt32LE(EVICTION_OFFSET.serverProtocolVersion),
  serverProtocolVersionMin:
    header.readUInt32LE(EVICTION_OFFSET.serverProtocolVersionMin)
});
