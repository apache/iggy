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
import type { RawClient, SendCommandOptions } from '../../client/client.type.js';
import { COMMAND_CODE } from '../command.code.js';
import { Consumer } from './offset.utils.js';
import { storeOffset } from './store-offset.command.js';
import { deleteOffset } from './delete-offset.command.js';

const context = { incarnation: 7n, ownerGeneration: 8n, metadataOp: 9n };
const target = { streamId: 1, topicId: 2, consumer: Consumer.Single, partitionId: 3 };

const recordingClient = (sent: { command: number, options?: SendCommandOptions }[]): RawClient => ({
  sendCommand: async (command: number, _payload: Buffer, options?: SendCommandOptions) => {
    sent.push({ command, options });
    return { status: 0, length: 0, data: Buffer.alloc(0) };
  }
} as unknown as RawClient);

describe('offset writes', () => {
  it('pass the caller context to the store and the delete', async () => {
    const sent: { command: number, options?: SendCommandOptions }[] = [];
    const client = recordingClient(sent);
    await storeOffset(async () => client)({ ...target, offset: 5n, context });
    await deleteOffset(async () => client)({ ...target, context });
    assert.deepEqual(sent, [
      { command: COMMAND_CODE.StoreOffset, options: { context } },
      { command: COMMAND_CODE.DeleteConsumerOffset, options: { context } }
    ]);
  });
});
