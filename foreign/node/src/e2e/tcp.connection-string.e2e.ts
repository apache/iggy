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

import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';
import type { TransportType } from '../client/client.type.js';
import { MAX_U32 } from '../constant.js';
import { getIggyAddress } from '../tcp.sm.utils.js';

const dummyOpt = 'nodelay=true' +
  '&reconnection_retries=1' +
  '&reconnection_interval=1s' +
  '&heartbeat_interval=10s' +
  '&tls=false';

/** Option-value variations exercised against a live server. */
const optionCases: {
  name: string,
  query: string,
  expect: {
    transport?: TransportType,
    reconnect?: Record<string, unknown>,
    heartbeatInterval?: number,
    options?: Record<string, unknown>
  }
}[] = [
  {
    name: 'unlimited retries at the default interval',
    query: 'reconnection_retries=unlimited',
    expect: {
      reconnect: { enabled: true, interval: 1000, maxRetries: MAX_U32 }
    }
  },
  {
    name: 'bounded retries with a sub-second interval',
    query: 'reconnection_retries=10&reconnection_interval=250ms',
    expect: {
      reconnect: { enabled: true, interval: 250, maxRetries: 10 }
    }
  },
  {
    name: 'compound duration interval',
    query: 'reconnection_interval=1m30s',
    expect: {
      reconnect: { enabled: true, interval: 90000, maxRetries: MAX_U32 }
    }
  },
  {
    name: 'disabled heartbeats and nodelay off',
    query: 'heartbeat_interval=0ms&nodelay=false',
    expect: {
      heartbeatInterval: 0,
      options: { noDelay: false }
    }
  },
  {
    name: 'reestablish_after validated then ignored',
    query: 'reestablish_after=7s',
    expect: {
      reconnect: { enabled: true, interval: 1000, maxRetries: MAX_U32 }
    }
  },
  {
    name: 'timer-ceiling heartbeat interval',
    query: 'heartbeat_interval=2147483647ms',
    expect: {
      heartbeatInterval: 2147483647
    }
  }
];

describe('e2e -> connection string', async () => {
  const [host, port] = getIggyAddress();
  const client = new Client(`iggy://iggy:iggy@${host}:${port}?${dummyOpt}`);

  it('e2e -> connection string::parses every option exactly once',
    () => {
      // A repeated key would silently keep only the last value, so each
      // option appears once above and must all land in the config.
      assert.equal(client._config.transport, 'TCP');
      assert.equal(client._config.options.noDelay, true);
      assert.deepEqual(client._config.reconnect, {
        enabled: true,
        interval: 1000,
        maxRetries: 1
      });
      assert.equal(client._config.heartbeatInterval, 10000);
    });

  it('e2e -> connection string::ping', async () => {
    assert.ok(await client.system.ping());
  });

  describe('option values', async () => {
    for (const { name, query, expect } of optionCases) {
      it(name, async () => {
        const caseClient =
          new Client(`iggy://iggy:iggy@${host}:${port}?${query}`);
        try {
          if (expect.transport !== undefined)
            assert.equal(caseClient._config.transport, expect.transport);
          if (expect.reconnect !== undefined)
            assert.deepEqual(
              caseClient._config.reconnect,
              expect.reconnect
            );
          if (expect.heartbeatInterval !== undefined)
            assert.equal(
              caseClient._config.heartbeatInterval,
              expect.heartbeatInterval
            );
          for (const [key, value] of Object.entries(expect.options ?? {}))
            assert.deepEqual(
              (caseClient._config.options as unknown as
                Record<string, unknown>)[key],
              value
            );

          // Every accepted value set must still reach a live server.
          assert.ok(await caseClient.system.ping());
        } finally {
          await caseClient.destroy();
        }
      });
    }
  });

  after(async () => {
    await client.destroy();
  });
});
