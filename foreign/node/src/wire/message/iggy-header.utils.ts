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

import { deserializeUUID, toDate } from "../serialize.utils.js";

export type IggyMessageHeader = {
  checksum: bigint,
  id: string | 0 | 0n,
  offset: bigint,
  timestamp: Date,
  originTimestamp: Date,
  userHeadersLength: number,
  payloadLength: number
};

// u64 + u128 + u64 + u64 + u64 + u32 + u32
export const IGGY_MESSAGE_HEADER_SIZE = 8 + 16 + 8 + 8 + 8 + 4 + 4;

export const serializeIggyMessageHeader = (
  id: Buffer,
  payload: Buffer,
  userHeaders: Buffer
) => {
  const b = Buffer.allocUnsafe(IGGY_MESSAGE_HEADER_SIZE);
  b.writeBigUInt64LE(0n, 0); // checksum u64
  b.fill(id, 8, 24); // id u128
  b.writeBigUInt64LE(0n, 24); // offset u64
  b.writeBigUInt64LE(0n, 32); // timestamp u64
  b.writeBigUint64LE(BigInt(new Date().getTime()), 40); // originTimestamp u64
  b.writeUInt32LE(userHeaders.length, 48); // userHeaders len u32
  b.writeUInt32LE(payload.length, 52) // payload len u32
  return b;
};

export const deserializeIggyMessageHeaders = (b: Buffer) => {
  if(b.length !== IGGY_MESSAGE_HEADER_SIZE)
    throw new Error(
      `deserialize message headers error, length = ${b.length} ` +
        `expected ${IGGY_MESSAGE_HEADER_SIZE}`
    );
  const headers: IggyMessageHeader = {
    checksum: b.readBigUInt64LE(0),
    id: deserializeUUID(b.subarray(8, 24)),
    offset: b.readBigUInt64LE(24),
    timestamp: toDate(b.readBigUInt64LE(32)),
    originTimestamp: toDate(b.readBigUInt64LE(40)),
    userHeadersLength: b.readUint32LE(48),
    payloadLength: b.readUint32LE(52)
  }
  return headers;
};
