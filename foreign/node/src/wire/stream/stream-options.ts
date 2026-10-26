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

import { createHash } from 'node:crypto';
import { inspect } from 'node:util';
import { COMMAND_CODE } from '../command.code.js';
import { responseError } from '../error.utils.js';
import type { OptionEntry } from '../options.utils.js';

const STREAM_KEY_BYTES = 32;
const INVALID_ENCRYPTION_KEY = 70;
const FINGERPRINT_LABEL = 'iggy/stream-key-fingerprint/v1';

/**
 * Options a stream takes at create.
 *
 * `encryptionKey` is the stream's own data key. It travels inside the create
 * request, so protect the connection with TLS. The server stores it wrapped
 * and never returns it, reads report only its source and fingerprints.
 * Without a key, a server with encryption enabled generates one for the
 * stream.
 */
export type StreamOptions = {
  /** The stream's data key, set once at create */
  encryptionKey?: StreamEncryptionKey,
  /**
   * Option keys with no field of their own, for a key the server catalog
   * gained after this build shipped. These entries print as given, so an
   * `encryption_key` entry is refused with error 70 before anything is sent.
   */
  options?: OptionEntry[]
};

/**
 * A stream data key: 32 random bytes. It prints only its fingerprint through
 * `util.inspect`, `JSON.stringify` and string conversion, so arguments
 * carrying it are safe to log. Never derive it from a password, because the
 * fingerprint is then an offline guessing oracle.
 */
export class StreamEncryptionKey {
  readonly #bytes: Buffer;

  private constructor(bytes: Buffer) {
    this.#bytes = bytes;
  }

  /**
   * @param key - 32 raw bytes, or their canonical padded base64. The bytes are copied
   * @throws ResponseError with code 70 (invalid encryption key) for any other input
   */
  static from(key: Buffer | string): StreamEncryptionKey {
    return new StreamEncryptionKey(Buffer.from(streamEncryptionKey(key, COMMAND_CODE.CreateStream)));
  }

  /** The value `getStream` reports as `encryptionKeyFingerprint` for this key */
  fingerprint(): string {
    return encryptionKeyFingerprint(this.#bytes);
  }

  /** A copy of the raw key, for the request encoder */
  asBytes(): Buffer {
    return Buffer.from(this.#bytes);
  }

  toString(): string {
    return `StreamEncryptionKey { fingerprint: ${this.fingerprint()} }`;
  }

  toJSON(): { fingerprint: string } {
    return { fingerprint: this.fingerprint() };
  }

  [inspect.custom](): string {
    return this.toString();
  }
}

/** Who chose a stream's at-rest key. */
export const EncryptionKeySource = {
  /** The client sent the key at create */
  Stream: 'stream',
  /** The server generated the key at create */
  Server: 'server',
  /** The stream is plaintext */
  None: 'none'
} as const;

export type EncryptionKeySource = typeof EncryptionKeySource[keyof typeof EncryptionKeySource];

/**
 * Fingerprint of a stream data key, the value `getStream` reports as
 * `encryptionKeyFingerprint`: the first 8 bytes of SHA-256 over a fixed label
 * and the key, as lowercase hex.
 *
 * @param key - 32 raw bytes, or their canonical padded base64
 * @returns 16 lowercase hex characters
 * @throws ResponseError with code 70 (invalid encryption key) for any other input
 */
export const encryptionKeyFingerprint = (key: Buffer | string): string =>
  createHash('sha256')
    .update(FINGERPRINT_LABEL)
    .update(streamEncryptionKey(key, 0))
    .digest()
    .subarray(0, 8)
    .toString('hex');

/**
 * The 32 key bytes of a stream key input. Text is accepted only as the
 * canonical padded standard base64 of 32 bytes, so whitespace, the URL-safe
 * alphabet, missing padding and noncanonical trailing bits are refused.
 *
 * @param key - 32 raw bytes, or their canonical padded base64
 * @param commandCode - Command the refusal is reported against
 * @returns The key bytes
 * @throws ResponseError with code 70 (invalid encryption key), naming no key bytes
 */
export const streamEncryptionKey = (key: Buffer | string, commandCode: number): Buffer => {
  const bytes = typeof key === 'string' ? Buffer.from(key, 'base64') : key;
  const canonical = typeof key !== 'string' || bytes.toString('base64') === key;
  if (!Buffer.isBuffer(bytes) || bytes.length !== STREAM_KEY_BYTES || !canonical)
    throw responseError(
      commandCode, INVALID_ENCRYPTION_KEY,
      `stream key must be ${STREAM_KEY_BYTES} bytes or their canonical padded base64`
    );
  return bytes;
};
