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

import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';
import { getIggyAddress } from '../tcp.sm.utils.js';

const dummyOpt = 'nodelay=true' +
  '&reconnection_retries=1' +
  '&reconnection_interval=1s' +
  '&heartbeat_interval=10s' +
  '&reconnection_retries=unlimited' +
  '&tls=false';

describe('e2e -> connection string', async () => {
  const [host, port] = getIggyAddress();
  const client = new Client(`iggy://iggy:iggy@${host}:${port}?${dummyOpt}`);

  it('e2e -> connection string::ping', async () => {
    assert.ok(await client.system.ping());
  });

  after(() => {
    client.destroy();
  });
});
