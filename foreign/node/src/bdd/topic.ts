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
import { When, Then } from "@cucumber/cucumber";
import type { TestWorld } from './world.js';


When(
  'I create a topic with name {string} in stream {int} with {int} partitions',
  async function (
    this: TestWorld,
    name: string,
    streamId: number,
    partitionCount: number
  ) {
    this.topic = await this.client.topic.create({
      name, streamId, partitionCount, compressionAlgorithm: 1
    });

  }
);

Then('the topic should be created successfully', function (this: TestWorld) {
  assert.ok(this.topic);
});

Then(
  'the topic should have name {string}',
  function (this: TestWorld, name: string) {
    assert.equal(this.topic.name, name);
  }
);

Then(
  'the topic should have {int} partitions',
  function (this: TestWorld,partitionCount: number) {
    assert.equal(this.topic.partitionsCount, partitionCount);
  }
);
