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
import { Then, When } from '@cucumber/cucumber';
import { BINARY_REQUEST_KIND, type BinaryRequestKind } from '../wire/command-set.js';
import type { TestWorld } from './world.js';

const REQUEST_KINDS: Record<string, BinaryRequestKind> = {
  non_replicated: BINARY_REQUEST_KIND.NonReplicated,
  replicated: BINARY_REQUEST_KIND.Replicated
};

When(
  'I send a raw {word} command with code {int} and an empty payload',
  async function (this: TestWorld, kind: string, code: number) {
    const requestKind = REQUEST_KINDS[kind];
    assert.ok(requestKind, `unknown binary request kind: ${kind}`);
    try {
      this.rawResponse = await this.client.sendBinaryRequest(requestKind, code, Buffer.alloc(0));
      this.rawError = undefined;
    } catch (error) {
      this.rawResponse = undefined;
      this.rawError = error instanceof Error ? error : new Error(String(error));
    }
  }
);

Then('the raw command should succeed with an empty response', function (this: TestWorld) {
  assert.equal(this.rawError, undefined);
  assert.deepEqual(this.rawResponse, Buffer.alloc(0));
});

Then('the raw command should succeed with a non-empty response', function (this: TestWorld) {
  assert.equal(this.rawError, undefined);
  assert.ok(this.rawResponse && this.rawResponse.length > 0);
});

Then('the raw command should fail with an invalid command error', function (this: TestWorld) {
  assert.equal(this.rawResponse, undefined);
  assert.match(this.rawError?.message ?? '', /code: 3, message: Invalid command/);
});

Then('the raw command should fail', function (this: TestWorld) {
  assert.equal(this.rawResponse, undefined);
  assert.ok(this.rawError);
});
