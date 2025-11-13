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

describe('e2e -> stream', async () => {

  const c = getTestClient();

  const name = 'e2e-tcp-stream';
  const name2 = `${name}-updated`;

  it('e2e -> stream::create', async () => {
    const stream = await c.stream.create({ name });
    assert.ok(stream);
  });

  it('e2e -> stream::list', async () => {
    const streams = await c.stream.list();
    assert.ok(streams.length > 0);
  });

  it('e2e -> stream::get', async () => {
    const stream = await c.stream.get({ streamId: name });
    assert.ok(stream);
  });

  it('e2e -> stream::update', async () => {
    const stream = await c.stream.update({
      streamId: name,
      name: name2
    });
    assert.ok(stream);
  });

  it('e2e -> stream::purge', async () => {
    assert.ok(await c.stream.purge({ streamId: name2 }));
  });

  it('e2e -> stream::delete', async () => {
    assert.ok(await c.stream.delete({ streamId: name2 }));
  });

  it('e2e -> stream::cleanup', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
