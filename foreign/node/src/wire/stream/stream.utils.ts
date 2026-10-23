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

import { toDate } from '../serialize.utils.js';
import { deserializePrefixedOptions, type ParsedOptions } from '../options.utils.js';
import { EncryptionKeySource } from './stream-options.js';

/**
 * Stream information returned from the server.
 */
export type Stream = {
  /** Stream ID */
  id: number,
  /** Stream name */
  name: string,
  /** Number of topics in the stream */
  topicsCount: number,
  /** Total size of the stream in bytes */
  sizeBytes: bigint,
  /** Total number of messages in the stream */
  messagesCount: bigint,
  /** Stream creation timestamp */
  createdAt: Date,
  /** Options the client explicitly sent at create */
  options: ParsedOptions,
  /** Options the server filled or computes, such as the encryption key source */
  derivedOptions: ParsedOptions,
  /** Who chose the stream's at-rest key, `null` when the server reported none */
  encryptionKeySource: EncryptionKeySource | null,
  /** Fingerprint of the stream's data key, `null` for a plaintext stream */
  encryptionKeyFingerprint: string | null,
  /** Fingerprints of the server keys wrapping the data key, empty for a plaintext stream */
  encryptionKekFingerprints: string[]
}

/**
 * Result of deserializing a stream.
 */
type StreamDeserialized = {
  /** Number of bytes consumed */
  bytesRead: number,
  /** Deserialized stream data */
  data: Stream
};

/**
 * Deserializes a stream from a buffer.
 *
 * @param r - Buffer containing serialized stream data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized stream data
 * @throws Error if the buffer is empty (stream does not exist)
 */
export const deserializeToStream = (r: Buffer, pos = 0): StreamDeserialized => {
  if (r.length === 0)
    throw new Error('Steam does not exist');

  const id = r.readUInt32LE(pos);
  const createdAt = toDate(r.readBigUint64LE(pos + 4));
  const topicsCount = r.readUInt32LE(pos + 12);
  const sizeBytes = r.readBigUint64LE(pos + 16);
  const messagesCount = r.readBigUint64LE(pos + 24);
  const nameLength = r.readUInt8(pos + 32);
  const name = r.subarray(pos + 33, pos + 33 + nameLength).toString();
  const explicit = deserializePrefixedOptions(r, pos + 33 + nameLength);
  const derived = deserializePrefixedOptions(r, pos + 33 + nameLength + explicit.bytesRead);
  const text = (key: string) => {
    const value = derived.options[key] ?? explicit.options[key];
    return typeof value === 'string' ? value : null;
  };
  const source = text('encryption_key_source');
  const keks = text('encryption_kek_fingerprint');

  return {
    bytesRead: 33 + nameLength + explicit.bytesRead + derived.bytesRead,
    data: {
      id, name, topicsCount, messagesCount, sizeBytes, createdAt,
      options: explicit.options,
      derivedOptions: derived.options,
      encryptionKeySource: Object.values<string>(EncryptionKeySource).includes(source ?? '')
        ? source as EncryptionKeySource : null,
      encryptionKeyFingerprint: text('encryption_key_fingerprint'),
      encryptionKekFingerprints: keks === null ? [] : keks.split(',')
    }
  };
};
