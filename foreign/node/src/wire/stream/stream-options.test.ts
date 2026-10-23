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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { randomBytes } from 'node:crypto';
import { format, inspect } from 'node:util';
import type { ClientProvider } from '../../client/client.type.js';
import { ResponseError } from '../error.utils.js';
import { HeaderValue } from '../message/header.utils.js';
import { StreamOptionKey } from '../options.utils.js';
import { CREATE_STREAM, createStream } from './create-stream.command.js';
import { StreamEncryptionKey, encryptionKeyFingerprint } from './stream-options.js';

const INVALID_ENCRYPTION_KEY = 70;
const INVALID_COMMAND = 3;

const keySpellings = (bytes: Buffer): string[] => [
  bytes.toString('hex'),
  bytes.toString('base64'),
  [...bytes.subarray(0, 4)].map((byte) => byte.toString(16).padStart(2, '0')).join(' '),
  [...bytes.subarray(0, 4)].join(', ')
];

describe('given stream key when printing create arguments should show only its fingerprint', () => {
  it('prints through inspect, format, JSON and string conversion without key bytes', () => {
    const bytes = randomBytes(32);
    const key = StreamEncryptionKey.from(bytes);
    const fingerprint = encryptionKeyFingerprint(bytes);
    const args = { name: 'orders', encryptionKey: key };
    const printed = [
      inspect(key),
      inspect(args, { depth: null, showHidden: true }),
      inspect(args, { depth: null, showHidden: true, customInspect: false }),
      format('%o %O %j %s', args, args, args, args),
      JSON.stringify(args),
      String(key),
      `${key}`
    ];
    for (const text of printed) {
      for (const spelling of keySpellings(bytes))
        assert.ok(!text.includes(spelling), `key bytes in ${text}`);
    }
    for (const text of printed.filter((_, index) => index !== 2))
      assert.ok(text.includes(fingerprint), `no fingerprint in ${text}`);
  });

  it('keeps its own copy of the key bytes', () => {
    const bytes = randomBytes(32);
    const fingerprint = encryptionKeyFingerprint(bytes);
    const key = StreamEncryptionKey.from(bytes);
    bytes.fill(0);
    assert.equal(key.fingerprint(), fingerprint);
    assert.equal(StreamEncryptionKey.from(key.asBytes()).fingerprint(), fingerprint);
  });

  it('refuses a plain buffer or string passed as the key, naming no key bytes', () => {
    const bytes = randomBytes(32);
    for (const input of [bytes, bytes.toString('base64')])
      assert.throws(
        () => CREATE_STREAM.serialize({
          name: 'orders', encryptionKey: input as unknown as StreamEncryptionKey
        }),
        (error: unknown) => {
          assert.ok(error instanceof ResponseError);
          assert.equal(error.errorCode, INVALID_ENCRYPTION_KEY);
          for (const spelling of keySpellings(bytes))
            assert.ok(!`${error.message} ${error.stack}`.includes(spelling));
          return true;
        }
      );
  });

  it('refuses a raw encryption_key options entry before network io, naming no key bytes', async () => {
    const bytes = randomBytes(32);
    const unexpectedNetwork: ClientProvider = () => {
      throw new Error('a raw stream key reached the network');
    };
    const redacted = (error: unknown) => {
      assert.ok(error instanceof ResponseError);
      assert.equal(error.errorCode, INVALID_COMMAND);
      assert.equal(error.fromServer, false);
      for (const spelling of keySpellings(bytes))
        assert.ok(!`${error.message} ${error.stack}`.includes(spelling));
      return true;
    };
    const rawEntries = [
      HeaderValue.Raw(bytes),
      HeaderValue.String(bytes.toString('base64'))
    ].map((value) => [{ key: StreamOptionKey.EncryptionKey, value }]);
    for (const options of rawEntries) {
      for (const encryptionKey of [undefined, StreamEncryptionKey.from(randomBytes(32))]) {
        const args = { name: 'orders', encryptionKey, options };
        assert.throws(() => CREATE_STREAM.serialize(args), redacted);
        await assert.rejects(createStream(unexpectedNetwork)(args), redacted);
      }
    }
  });
});
