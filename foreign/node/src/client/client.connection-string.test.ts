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
import {
  parseConnectionString,
  parseDuration
} from './client.connection-string.js';
import {
  DEFAULT_HEARTBEAT_INTERVAL,
  normalizeClientConfig
} from './client.config.js';

describe('parseConnectionString', () => {
  it('parses the default scheme with password credentials', () => {
    assert.deepEqual(
      parseConnectionString('iggy://iggy:secret@127.0.0.1:8090'),
      {
        transport: 'TCP',
        options: { host: '127.0.0.1', port: 8090 },
        credentials: { username: 'iggy', password: 'secret' }
      }
    );
  });

  it('parses the explicit tcp scheme with a personal access token', () => {
    assert.deepEqual(
      parseConnectionString('iggy+tcp://iggypat-1234567890abcdef@localhost:8090'),
      {
        transport: 'TCP',
        options: { host: 'localhost', port: 8090 },
        credentials: { token: 'iggypat-1234567890abcdef' }
      }
    );
  });

  it('maps tls options to the TLS transport', () => {
    assert.deepEqual(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090?tls=true&tls_domain=iggy.apache.org'
      ),
      {
        transport: 'TLS',
        options: {
          host: 'localhost',
          port: 8090,
          servername: 'iggy.apache.org'
        },
        credentials: { username: 'iggy', password: 'secret' }
      }
    );
  });

  it('maps reconnection and heartbeat options', () => {
    assert.deepEqual(
      parseConnectionString(
        'iggy+tcp://iggy:secret@localhost:8090' +
        '?reconnection_retries=3&reconnection_interval=5s&heartbeat_interval=10s'
      ),
      {
        transport: 'TCP',
        options: { host: 'localhost', port: 8090 },
        credentials: { username: 'iggy', password: 'secret' },
        reconnect: {
          enabled: true,
          maxRetries: 3,
          interval: 5000
        },
        heartbeatInterval: 10000
      }
    );
  });

  it('maps nodelay to the socket option', () => {
    assert.equal(
      parseConnectionString('iggy://iggy:secret@localhost:8090?nodelay=true')
        .options.noDelay,
      true
    );
  });

  it('maps unlimited retries to a safe integer ceiling', () => {
    assert.equal(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090?reconnection_retries=unlimited'
      ).reconnect?.maxRetries,
      Number.MAX_SAFE_INTEGER
    );
  });

  it('ignores reestablish_after for format compatibility', () => {
    assert.deepEqual(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090?reestablish_after=10s'
      ),
      {
        transport: 'TCP',
        options: { host: 'localhost', port: 8090 },
        credentials: { username: 'iggy', password: 'secret' }
      }
    );
  });

  it('rejects unsupported transports', () => {
    for (const value of [
      'iggy+quic://iggy:secret@localhost:8090',
      'iggy+ws://iggy:secret@localhost:8090'
    ])
      assert.throws(
        () => parseConnectionString(value),
        /unsupported transport/
      );
  });

  it('rejects malformed connection strings', () => {
    for (const value of [
      '',
      'iggy',
      'iggy://',
      'iggy://:secret@localhost:8090',
      'iggy://iggy:@localhost:8090',
      'iggy://iggy:secret@localhost',
      'iggy://iggy:secret@:8090',
      'iggy://iggy:secret@localhost:port',
      'iggy://iggy:secret@localhost:70000',
      'iggy://iggy:secret@localhost:8090?unknown=value',
      'iggy://iggy:secret@localhost:8090?tls=maybe',
      'iggy://iggy:secret@localhost:8090?reconnection_retries=three'
    ])
      assert.throws(() => parseConnectionString(value), TypeError);
  });

  it('parses IPv6 host addresses', () => {
    assert.deepEqual(
      parseConnectionString('iggy://iggy:secret@[::1]:8090').options,
      { host: '[::1]', port: 8090 }
    );
  });
});

describe('parseDuration', () => {
  it('converts supported units to milliseconds', () => {
    assert.equal(parseDuration('500ms'), 500);
    assert.equal(parseDuration('5s'), 5000);
    assert.equal(parseDuration('2m'), 120000);
    assert.equal(parseDuration('1h'), 3600000);
    assert.equal(parseDuration('0.5s'), 500);
  });

  it('rejects unsupported durations', () => {
    for (const value of ['5', '5d', 's', '-1s', 'ms'])
      assert.throws(() => parseDuration(value), /invalid duration/);
  });
});

describe('normalizeClientConfig with connection strings', () => {
  it('applies client defaults to the parsed config', () => {
    const normalized = normalizeClientConfig('iggy://iggy:secret@localhost:8090');

    assert.equal(normalized.transport, 'TCP');
    assert.equal(normalized.options.host, 'localhost');
    assert.equal(normalized.options.port, 8090);
    assert.deepEqual(normalized.credentials, {
      username: 'iggy',
      password: 'secret'
    });
    assert.equal(normalized.heartbeatInterval, DEFAULT_HEARTBEAT_INTERVAL);
    assert.deepEqual(normalized.poolSize, { min: 1, max: 1 });
  });
});
