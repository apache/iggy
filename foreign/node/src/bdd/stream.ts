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
import { Given, When, Then } from "@cucumber/cucumber";
import type { TestWorld } from './world.js';

Given('I have no streams in the system', async function (this: TestWorld) {
  assert.deepEqual([], await this.client.stream.list());
});

When(
  'I create a stream with name {string}',
  async function (this: TestWorld, name: string) {
    this.stream = await this.client.stream.create({ name })
    return this.stream;
  }
);

Then('the stream should be created successfully', function () {
  assert.ok(this.stream);
});

Then(
  'the stream should have name {string}',
  async function (this: TestWorld, name: string) {
    assert.equal(this.stream.name, name);
  }
);

When(
  'I update the stream name to {string}',
  async function (this: TestWorld, newName: string) {
    assert.ok(await this.client.stream.update({
      streamId: this.stream.id,
      name: newName
    }));
    this.stream = { ...this.stream, name: newName };
  }
);

Then(
  'the stream name should be updated to {string}',
  async function (this: TestWorld, expectedName: string) {
    const stream = await this.client.stream.get({ streamId: this.stream.id });
    assert.ok(stream, 'Stream should exist after update');
    assert.equal(stream!.name, expectedName);
  }
);

When(
  'I delete the stream',
  async function (this: TestWorld) {
    assert.ok(await this.client.stream.delete({ streamId: this.stream.id }));
  }
);

Then(
  'the stream should be deleted successfully',
  async function (this: TestWorld) {
    // If we reached here without error, the stream was deleted successfully
    assert.ok(true);
  }
);

// Cleanup: delete stream after test
Then(
  'I can delete stream with ID {int}',
  async function (this: TestWorld, streamId: number) {
    assert.ok(await this.client.stream.delete({streamId}));
  }
);
