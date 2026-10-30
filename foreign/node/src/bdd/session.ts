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
import { Given, Then, When } from '@cucumber/cucumber';
import { Client } from '../client/index.js';
import { OPERATION_TIMEOUT_MS } from '../client/client.operation.js';
import type { TestWorld } from './world.js';
import { getNoQuorumAddress, getRootCredentials } from './env.js';

const recordCallError = async (world: TestWorld, call: () => Promise<unknown>) => {
  try {
    await call();
    world.callError = undefined;
  } catch (error) {
    world.callError = error instanceof Error ? error : new Error(String(error));
  }
};

Given('I have a cluster node that cannot reach quorum', function (this: TestWorld) {
  const [host, port] = getNoQuorumAddress();
  this.client = new Client({
    transport: 'TCP',
    options: { host, port },
    credentials: getRootCredentials()
  });
});

When('I sign out', async function (this: TestWorld) {
  assert.ok(await this.client.session.logout());
});

When('I request the stream with ID {int}', async function (this: TestWorld, streamId: number) {
  await recordCallError(this, () => this.client.stream.get({ streamId }));
});

// The sign-in runs until the SDK deadline, past cucumber's default step timeout.
When(
  'I sign in to that node as the root user',
  { timeout: 2 * OPERATION_TIMEOUT_MS },
  async function (this: TestWorld) {
    await recordCallError(this, () => this.client.session.login(getRootCredentials()));
  }
);

Then('the call should fail with error code {int}', function (this: TestWorld, code: number) {
  assert.ok(this.callError, 'the call should have failed');
  assert.equal((this.callError as { errorCode?: number }).errorCode, code, this.callError.message);
});
