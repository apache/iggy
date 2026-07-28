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
import { getTestClient } from './test-client.utils.js';
import { BINARY_REQUEST_KIND } from '../wire/command-set.js';
import { COMMAND_CODE } from '../wire/command.code.js';

describe('e2e -> raw', async () => {

  const c = getTestClient();

  const VENDOR_CODE = 60_001;

  it('e2e -> raw::ping', async () => {
    const response = await c.sendBinaryRequest(
      BINARY_REQUEST_KIND.NonReplicated,
      COMMAND_CODE.Ping,
      Buffer.alloc(0)
    );
    assert.deepEqual(response, Buffer.alloc(0));
  });

  it('e2e -> raw::getStats', async () => {
    const response = await c.sendBinaryRequest(
      BINARY_REQUEST_KIND.NonReplicated,
      COMMAND_CODE.GetStats,
      Buffer.alloc(0)
    );
    assert.ok(response.length > 0);
  });

  it('e2e -> raw::getStatsUsesTheProtocolKindRules', async () => {
    const request = () => c.sendBinaryRequest(
      BINARY_REQUEST_KIND.Replicated,
      COMMAND_CODE.GetStats,
      Buffer.alloc(0)
    );
    if (process.env.IGGY_TEST_PROTOCOL === 'vsr') {
      await assert.rejects(request);
      return;
    }

    // Classic framing has no operation field, so the read still succeeds.
    assert.ok((await request()).length > 0);
  });

  it('e2e -> raw::sessionControlCodeRejectedClientSide', async () => {
    for (const kind of Object.values(BINARY_REQUEST_KIND))
      await assert.rejects(
        () => c.sendBinaryRequest(kind, COMMAND_CODE.LoginUser, Buffer.alloc(0))
      );
  });

  it('e2e -> raw::vendorCodeRejectedByServer', async () => {
    await assert.rejects(
      () => c.sendBinaryRequest(BINARY_REQUEST_KIND.NonReplicated, VENDOR_CODE, Buffer.alloc(0))
    );

    // The rejection is request-level, so the connection stays usable.
    const response = await c.sendBinaryRequest(
      BINARY_REQUEST_KIND.NonReplicated,
      COMMAND_CODE.Ping,
      Buffer.alloc(0)
    );
    assert.deepEqual(response, Buffer.alloc(0));
  });

  it('e2e -> raw::repeatedVendorCodesDoNotGapMetadataRequestIds', async () => {
    for (let attempt = 0; attempt < 3; attempt += 1)
      await assert.rejects(
        () => c.sendBinaryRequest(
          BINARY_REQUEST_KIND.NonReplicated,
          VENDOR_CODE + attempt,
          Buffer.from([attempt])
        )
      );

    const streamName = `e2e-raw-sequence-${Date.now()}`;
    const stream = await c.stream.create({ name: streamName });
    assert.equal(stream.name, streamName);
    assert.equal(await c.stream.delete({ streamId: streamName }), true);
  });

  it('e2e -> raw::unknownReplicatedCodeRejected', async () => {
    await assert.rejects(
      () => c.sendBinaryRequest(
        BINARY_REQUEST_KIND.Replicated,
        VENDOR_CODE,
        Buffer.alloc(0)
      )
    );
  });

  after(() => {
    c.destroy();
  });
});
