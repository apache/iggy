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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import type { ClientConfig } from './client.type.js';
import {
  DEFAULT_MAX_RESPONSE_FRAME_SIZE,
  normalizeClientConfig
} from './client.config.js';

const config = (): ClientConfig => ({
  transport: 'TCP',
  options: { host: '127.0.0.1', port: 8090 },
  credentials: { username: 'iggy', password: 'iggy' }
});

describe('normalizeClientConfig', () => {
  it('defaults to classic without changing classic pool sizing', () => {
    const normalized = normalizeClientConfig({
      ...config(),
      poolSize: { min: 2, max: 4 }
    });

    assert.equal(normalized.protocol, 'classic');
    assert.deepEqual(normalized.poolSize, { min: 2, max: 4 });
    assert.equal(
      normalized.maxResponseFrameSize,
      DEFAULT_MAX_RESPONSE_FRAME_SIZE
    );
  });

  it('restricts VSR to one pooled connection', () => {
    const normalized = normalizeClientConfig({
      ...config(),
      protocol: 'vsr'
    });
    assert.deepEqual(normalized.poolSize, { min: 1, max: 1 });

    assert.throws(
      () => normalizeClientConfig({
        ...config(),
        protocol: 'vsr',
        poolSize: { max: 2 }
      }),
      /exactly one pooled connection/
    );
  });

  it('rejects invalid protocols before opening a socket', () => {
    assert.throws(
      () => normalizeClientConfig({
        ...config(),
        protocol: 'auto' as 'vsr'
      }),
      /unsupported wire protocol/
    );
  });

  it('supports VSR over TLS', () => {
    const normalized = normalizeClientConfig({
      ...config(),
      protocol: 'vsr',
      transport: 'TLS'
    });

    assert.equal(normalized.protocol, 'vsr');
    assert.equal(normalized.transport, 'TLS');
    assert.deepEqual(normalized.poolSize, { min: 1, max: 1 });
  });

  it('rejects unsafe response frame limits', () => {
    for (const maxResponseFrameSize of [0, 255, 1.5, Number.MAX_VALUE])
      assert.throws(
        () => normalizeClientConfig({
          ...config(),
          maxResponseFrameSize
        }),
        /maxResponseFrameSize/
      );
  });
});
