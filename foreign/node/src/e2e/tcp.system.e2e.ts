/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { getTestClient } from './test-client.utils.js';

describe('e2e -> system', async () => {


  const c = getTestClient();

  it('e2e -> system::ping', async () => {
    assert.ok(await c.system.ping());
  });

  it('e2e -> system::login', async () => {
    assert.deepEqual(
      await c.session.login({ username: 'iggy', password: 'iggy' }),
      { userId: 0 }
    )
  });

  it('e2e -> system::getStat', async () => {
    assert.deepEqual(
      Object.keys(await c.system.getStats()),
      [
        'processId', 'cpuUsage', 'totalCpuUsage', 'memoryUsage', 'totalMemory',
        'availableMemory', 'runTime', 'startTime', 'readBytes', 'writtenBytes',
        'messagesSizeBytes', 'streamsCount', 'topicsCount', 'partitionsCount',
        'segmentsCount', 'messagesCount', 'clientsCount', 'consumersGroupsCount',
        'hostname', 'osName', 'osVersion', 'kernelVersion'
      ]
    );
  });

  it('e2e -> system::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
