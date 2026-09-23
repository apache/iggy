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
import { MAX_U32 } from '../constant.js';
import type { ClientConfig } from './client.type.js';
import {
  DEFAULT_HEARTBEAT_INTERVAL,
  DEFAULT_MAX_RESPONSE_FRAME_SIZE,
  normalizeClientConfig
} from './client.config.js';

const config = (): ClientConfig => ({
  transport: 'TCP',
  options: { host: '127.0.0.1', port: 8090 },
  credentials: { username: 'iggy', password: 'iggy' }
});

describe('normalizeClientConfig', () => {
  it('applies the default response frame limit', () => {
    const normalized = normalizeClientConfig(config());

    assert.equal(
      normalized.maxResponseFrameSize,
      DEFAULT_MAX_RESPONSE_FRAME_SIZE
    );
  });

  it('enables the heartbeat by default and honours an explicit interval', () => {
    assert.equal(DEFAULT_HEARTBEAT_INTERVAL, 5000);

    assert.equal(
      normalizeClientConfig(config()).heartbeatInterval,
      DEFAULT_HEARTBEAT_INTERVAL
    );

    assert.equal(
      normalizeClientConfig({ ...config(), heartbeatInterval: 1000 })
        .heartbeatInterval,
      1000
    );

    assert.equal(
      normalizeClientConfig({ ...config(), heartbeatInterval: 0 })
        .heartbeatInterval,
      0
    );
  });

  it('rejects unusable heartbeat intervals', () => {
    for (const heartbeatInterval of [
      -1, -5000, Number.NaN, 1.5, 2_147_483_648, 2 ** 32, Number.MAX_VALUE
    ])
      assert.throws(
        () => normalizeClientConfig({
          ...config(),
          heartbeatInterval
        }),
        /heartbeatInterval/
      );
  });

  it('accepts a usable reconnect interval', () => {
    const reconnect = { enabled: true, interval: 1000, maxRetries: 3 };
    assert.deepEqual(
      normalizeClientConfig({ ...config(), reconnect }).reconnect,
      reconnect
    );
  });

  it('rejects unusable reconnect intervals', () => {
    for (const interval of [
      0, -1000, Number.NaN, 1.5, 2_147_483_648, Number.MAX_VALUE
    ])
      assert.throws(
        () => normalizeClientConfig({
          ...config(),
          reconnect: { enabled: true, interval, maxRetries: 1 }
        }),
        /reconnect\.interval/
      );
  });

  it('skips the interval check when reconnect is disabled', () => {
    assert.doesNotThrow(() =>
      normalizeClientConfig({
        ...config(),
        reconnect: { enabled: false, interval: 0, maxRetries: 0 }
      })
    );
  });

  it('rejects unusable reconnect maxRetries', () => {
    for (const maxRetries of [
      -1, Number.NaN, 1.5, MAX_U32 + 1, Number.MAX_VALUE
    ])
      assert.throws(
        () => normalizeClientConfig({
          ...config(),
          reconnect: { enabled: true, interval: 1000, maxRetries }
        }),
        /reconnect\.maxRetries/
      );
  });

  it('restricts the client to one pooled connection', () => {
    const normalized = normalizeClientConfig(config());
    assert.deepEqual(normalized.poolSize, { min: 1, max: 1 });

    assert.throws(
      () => normalizeClientConfig({
        ...config(),
        poolSize: { max: 2 }
      }),
      /exactly one pooled connection/
    );
  });

  it('supports TLS transport', () => {
    const normalized = normalizeClientConfig({
      ...config(),
      transport: 'TLS'
    });

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
