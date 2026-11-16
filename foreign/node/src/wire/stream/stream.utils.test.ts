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
import { deserializeToStream } from './stream.utils.js';
import { toDate } from '../serialize.utils.js';

const buildStreamRecord = ({
  id = 42,
  createdAtMicros = 1_710_000_000_000_000n,
  topicsCount = 3,
  sizeBytes = 8192n,
  messagesCount = 250n,
  name = 'my-test-stream',
  explicitOptions = Buffer.alloc(4),
  derivedOptions = Buffer.alloc(4)
}: {
  id?: number,
  createdAtMicros?: bigint,
  topicsCount?: number,
  sizeBytes?: bigint,
  messagesCount?: bigint,
  name?: string,
  explicitOptions?: Buffer,
  derivedOptions?: Buffer
} = {}): Buffer => {
  const nameBuffer = Buffer.from(name, 'utf8');
  const fixed = Buffer.alloc(33);
  fixed.writeUInt32LE(id, 0);
  fixed.writeBigUInt64LE(createdAtMicros, 4);
  fixed.writeUInt32LE(topicsCount, 12);
  fixed.writeBigUInt64LE(sizeBytes, 16);
  fixed.writeBigUInt64LE(messagesCount, 24);
  fixed.writeUInt8(nameBuffer.length, 32);

  return Buffer.concat([
    fixed,
    nameBuffer,
    explicitOptions,
    derivedOptions
  ]);
};

describe('deserializeToStream', () => {
  it('throws when buffer is empty with "Stream does not exist"', () => {
    assert.throws(
      () => deserializeToStream(Buffer.alloc(0)),
      { message: 'Stream does not exist' }
    );
  });

  it('deserializes a stream record with empty option blocks', () => {
    const streamBuffer = buildStreamRecord({
      id: 101,
      createdAtMicros: 1_700_000_000_000_000n,
      topicsCount: 5,
      sizeBytes: 4096n,
      messagesCount: 50n,
      name: 'orders-stream'
    });

    const result = deserializeToStream(streamBuffer);

    assert.equal(result.bytesRead, streamBuffer.length);
    assert.equal(result.data.id, 101);
    assert.equal(result.data.name, 'orders-stream');
    assert.equal(result.data.topicsCount, 5);
    assert.equal(result.data.sizeBytes, 4096n);
    assert.equal(result.data.messagesCount, 50n);
    assert.deepEqual(result.data.createdAt, toDate(1_700_000_000_000_000n));
    assert.deepEqual(result.data.options, {});
    assert.deepEqual(result.data.derivedOptions, {});
    assert.equal(result.data.encryptionKeySource, null);
    assert.equal(result.data.encryptionKeyFingerprint, null);
    assert.deepEqual(result.data.encryptionKekFingerprints, []);
  });

  it('reads the same record correctly from a non-zero offset', () => {
    const streamBuffer = buildStreamRecord({
      id: 202,
      createdAtMicros: 1_720_000_000_000_000n,
      topicsCount: 1,
      sizeBytes: 1024n,
      messagesCount: 10n,
      name: 'offset-stream'
    });

    const offset = 16;
    const prefix = Buffer.alloc(offset, 0xaa);
    const combinedBuffer = Buffer.concat([prefix, streamBuffer]);

    const resultFromOffset = deserializeToStream(combinedBuffer, offset);
    const directResult = deserializeToStream(streamBuffer, 0);

    assert.equal(resultFromOffset.bytesRead, directResult.bytesRead);
    assert.deepEqual(resultFromOffset.data, directResult.data);
  });
});
