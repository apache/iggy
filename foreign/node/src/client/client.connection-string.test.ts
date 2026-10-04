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
        credentials: { username: 'iggy', password: 'secret' },
        reconnect: { enabled: true, interval: 1000, maxRetries: MAX_U32 }
      }
    );
  });

  it('parses the explicit tcp scheme with a personal access token', () => {
    assert.deepEqual(
      parseConnectionString('iggy+tcp://iggypat-1234567890abcdef@localhost:8090'),
      {
        transport: 'TCP',
        options: { host: 'localhost', port: 8090 },
        credentials: { token: 'iggypat-1234567890abcdef' },
        reconnect: { enabled: true, interval: 1000, maxRetries: MAX_U32 }
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
        credentials: { username: 'iggy', password: 'secret' },
        reconnect: { enabled: true, interval: 1000, maxRetries: MAX_U32 }
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

  it('applies unlimited/1s reconnection defaults to partial options', () => {
    // retries alone keep the 1s interval; interval alone keeps unlimited.
    assert.deepEqual(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090?reconnection_retries=3'
      ).reconnect,
      { enabled: true, interval: 1000, maxRetries: 3 }
    );
    assert.deepEqual(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090?reconnection_interval=5s'
      ).reconnect,
      { enabled: true, interval: 5000, maxRetries: MAX_U32 }
    );
  });

  it('maps nodelay to the socket option', () => {
    assert.equal(
      parseConnectionString('iggy://iggy:secret@localhost:8090?nodelay=true')
        .options.noDelay,
      true
    );
  });

  it('maps unlimited retries to the u32 ceiling', () => {
    assert.equal(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090?reconnection_retries=unlimited'
      ).reconnect?.maxRetries,
      MAX_U32
    );
  });

  it('accepts retry counts up to u32::MAX and rejects overflow', () => {
    assert.equal(
      parseConnectionString(
        `iggy://iggy:secret@localhost:8090?reconnection_retries=${MAX_U32}`
      ).reconnect?.maxRetries,
      MAX_U32
    );
    for (const value of [
      'iggy://iggy:secret@localhost:8090?reconnection_retries=4294967296',
      'iggy://iggy:secret@localhost:8090?reconnection_retries=99999999999999'
    ])
      assert.throws(() => parseConnectionString(value), TypeError);
  });

  it('rejects a non-positive reconnection interval', () => {
    // Zero spellings parse but are rejected by the positivity bound.
    for (const value of ['0', '0ms', 'none'])
      assert.throws(
        () =>
          parseConnectionString(
            `iggy://iggy:secret@localhost:8090?reconnection_interval=${value}`
          ),
        /must be positive/
      );
    // Negative durations shall not parse
    assert.throws(
      () =>
        parseConnectionString(
          'iggy://iggy:secret@localhost:8090?reconnection_interval=-1s'
        ),
      TypeError
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
        credentials: { username: 'iggy', password: 'secret' },
        reconnect: { enabled: true, interval: 1000, maxRetries: MAX_U32 }
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
      'iggy://iggy:secret@localhost:8090?reconnection_retries=three',
      'iggy://iggy:secret@[::1:8090',
      'iggy://iggy:secret@[]:8090',
      'iggy://iggy:secret@[::1]x:8090',
      'iggy://iggy:secret@2001:db8::1:8090',
      'iggy://iggy:secret@host:8090:9090',
      'iggy://iggy:secret@localhost:8090?',
      'iggy://iggy:secret@localhost:8090?&',
      'iggy://iggy:secret@localhost:8090?reestablish_after=garbage'
    ])
      assert.throws(() => parseConnectionString(value), TypeError);
  });

  it('never includes the connection string in error messages', () => {
    const secrets = ['hunter2', 'iggypat-1234567890abcdef'];
    for (const value of [
      'iggy://iggy:hunter2@localhost',
      `iggy+tcp://iggypat-1234567890abcdef@localhost`,
      'iggy://iggy:hunter2@localhost:8090?unknown=value',
      'iggy://iggy:hunter2@localhost:8090?tls=maybe',
      'iggy://iggy:hunter2@localhost:8090?reconnection_retries=three',
      'iggy://iggy:hunter2@localhost:70000'
    ]) {
      try {
        parseConnectionString(value);
        assert.fail(`expected "${value}" to be rejected`);
      } catch (error) {
        assert.ok(error instanceof TypeError);
        for (const secret of secrets)
          assert.ok(
            !error.message.includes(secret),
            `error message leaked a secret: ${error.message}`
          );
      }
    }
  });

  it('parses IPv6 host addresses without their brackets', () => {
    assert.deepEqual(
      parseConnectionString('iggy://iggy:secret@[::1]:8090').options,
      { host: '::1', port: 8090 }
    );
  });

  it('stores tls_ca_file as a path without reading it at parse time', () => {
    assert.deepEqual(
      parseConnectionString(
        'iggy://iggy:secret@localhost:8090' +
        '?tls=true&tls_ca_file=/does/not/exist.pem'
      ).options,
      {
        host: 'localhost',
        port: 8090,
        caFile: '/does/not/exist.pem'
      }
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
    assert.equal(parseDuration('1h 1m 1s'), 3661000);
    assert.equal(parseDuration('1h30m'), 5400000);
    assert.equal(parseDuration('5d'), 432000000);
    assert.equal(parseDuration('2w'), 1209600000);
    assert.equal(parseDuration('1y'), 31557600000);
    assert.equal(parseDuration('5sec'), 5000);
    assert.equal(parseDuration('5msec'), 5);
    // Fractional results are rounded to whole milliseconds.
    assert.equal(parseDuration('1.005s'), 1005);
    assert.equal(parseDuration('5usec'), 0);
    assert.equal(parseDuration('500nsec'), 0);
    for (const zero of ['0', 'unlimited', 'disabled', 'none', 'UNLIMITED'])
      assert.equal(parseDuration(zero), 0);
  });

  it('rejects unsupported durations', () => {
    for (const value of ['5', '-1s', 'ms', '', 'abc', 's'])
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

  it('rejects reconnect intervals beyond the node timer ceiling', () => {
    // Parses to 3_600_000_000 ms; setInterval would clamp it back to 1 ms.
    assert.throws(
      () =>
        normalizeClientConfig(
          'iggy://iggy:secret@localhost:8090?reconnection_interval=1000h'
        ),
      /reconnect\.interval/
    );
  });
});
