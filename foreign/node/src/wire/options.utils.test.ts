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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  serializeOptions,
  deserializePrefixedOptions
} from './options.utils.js';
import { HeaderValue } from './message/header.utils.js';

const prefixed = (block: Buffer): Buffer => {
  const length = Buffer.alloc(4);
  length.writeUInt32LE(block.length, 0);
  return Buffer.concat([length, block]);
};

/**
 * The cross-SDK golden vector for an options block.
 *
 * Rust pins the identical bytes in `core/binary_protocol/src/primitives/options.rs`,
 * as do the Go and Java SDKs. Round-tripping through this SDK's own decoder proves
 * nothing about interoperability; these bytes are the contract.
 */
const GOLDEN_OPTIONS_BLOCK = Buffer.from([
  2, 13, 0, 0, 0,
  ...Buffer.from('enforce_fsync'),
  3, 1, 0, 0, 0, 1,
  2, 12, 0, 0, 0,
  ...Buffer.from('segment_size'),
  12, 8, 0, 0, 0,
  0, 0, 0, 64, 0, 0, 0, 0
]);

describe('serializeOptions', () => {
  it('encodes the cross-SDK golden vector byte for byte', () => {
    const encoded = serializeOptions([
      { key: 'enforce_fsync', value: HeaderValue.Bool(true) },
      { key: 'segment_size', value: HeaderValue.Uint64(1_073_741_824n) }
    ]);

    assert.deepEqual(encoded, GOLDEN_OPTIONS_BLOCK);
  });
});

describe('deserializePrefixedOptions', () => {
  it('reads a whole block and reports the bytes it consumed', () => {
    const block = prefixed(serializeOptions([
      { key: 'segment_size', value: HeaderValue.Uint64(1_048_576n) }
    ]));

    const { bytesRead, options } = deserializePrefixedOptions(block);

    assert.equal(bytesRead, block.length);
    assert.deepEqual(options, { segment_size: 1_048_576n });
  });

  it('rejects a block whose declared length runs past the payload', () => {
    // `subarray` clamps instead of throwing, so without the bounds check the
    // truncated value comes back as raw bytes through the forward-compat catch
    // and `bytesRead` over-reports, shifting every later field.
    const block = prefixed(serializeOptions([
      { key: 'segment_size', value: HeaderValue.Uint64(1_048_576n) }
    ]));

    assert.throws(
      () => deserializePrefixedOptions(block.subarray(0, block.length - 4)),
      /overruns the payload/
    );
  });
});
