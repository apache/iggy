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


import assert from 'node:assert/strict';
import { Client } from '../client/index.js';
import { Given } from "@cucumber/cucumber";
import type { TestWorld } from './world.js';
import { getIggyAddress } from '../tcp.sm.utils.js';

const credentials = { username: 'iggy', password: 'iggy' };
const [host, port] = getIggyAddress();

const opt = {
  transport: 'TCP' as const,
  options: { host, port },
  credentials
};

Given('I have a running Iggy server', function () {
  return true;
});


Given('I am authenticated as the root user', async function (this: TestWorld) {
  this.client = new Client(opt);
  assert.deepEqual({ userId: 0 }, await this.client.session.login(credentials));
  assert.equal(true, await this.client.system.ping());
});
