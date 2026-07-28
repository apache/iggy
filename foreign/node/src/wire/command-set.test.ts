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

import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { SimpleClient } from '../client/client.js';
import type { RawClient } from '../client/client.type.js';
import { COMMAND_CODE } from './command.code.js';
import {
  BINARY_REQUEST_KIND,
  BinaryRequestKind,
  type BinaryRequestKind as BinaryRequestKindType
} from './command-set.js';

const mockRawClient = (): RawClient => ({
  protocol: 'classic',
  sendCommand: async () => {
    throw new Error('sendCommand should not be called by the session-control guard');
  },
  isAuthenticated: true,
  authenticate: async () => {
    throw new Error('authenticate should not be called by the session-control guard');
  },
  destroy: () => {},
  on: () => {},
  once: () => {},
  getReadStream: () => {
    throw new Error('getReadStream should not be called by the session-control guard');
  },
});

describe('CommandAPI.sendBinaryRequest', () => {
  it('exports exactly the two protocol request kinds', () => {
    assert.deepEqual(BinaryRequestKind, {
      NonReplicated: 'non_replicated',
      Replicated: 'replicated'
    });
  });

  it('requires the request kind in TypeScript', () => {
    const client = new SimpleClient(mockRawClient());
    if (false) {
      // @ts-expect-error the breaking API requires a replication kind
      void client.sendBinaryRequest(COMMAND_CODE.Ping, Buffer.alloc(0));
    }
    assert.equal('sendBinaryRequestWithKind' in client, false);
  });

  describe('session-control guard', () => {

    Object.values(BINARY_REQUEST_KIND).forEach((kind) => {
      [
        COMMAND_CODE.LoginUser,
        COMMAND_CODE.LogoutUser,
        COMMAND_CODE.LoginRegister,
        COMMAND_CODE.LoginWithAccessToken,
        COMMAND_CODE.LoginRegisterWithAccessToken,
      ].forEach((code) => {
        it(`rejects ${kind} code ${code} before reaching the client provider`, async () => {
          const client = new SimpleClient(mockRawClient());
          await assert.rejects(
            () => client.sendBinaryRequest(kind, code, Buffer.alloc(0)),
            /code: 3, message: Invalid command/
          );
        });
      });
    });

  });

  it('rejects every invalid request kind before reaching the raw client', async () => {
    const client = new SimpleClient(mockRawClient());
    const invalidKinds = [
      undefined,
      null,
      'auto',
      0,
      {},
    ];
    for (const invalidKind of invalidKinds)
      await assert.rejects(
        () => client.sendBinaryRequest(
          invalidKind as BinaryRequestKindType,
          COMMAND_CODE.Ping,
          Buffer.alloc(0)
        ),
        /code: 3, message: Invalid command/
      );
  });

  it('encodes both kinds identically because classic framing has no operation field', async () => {
    const customCode = 60_001;
    const payload = Buffer.from([0xAA, 0xBB, 0xCC]);
    const frames: { code: number, payload: Buffer }[] = [];
    const raw = mockRawClient();
    raw.sendCommand = async (code, sentPayload) => {
      frames.push({ code, payload: Buffer.from(sentPayload) });
      return { status: 0, length: 1, data: Buffer.alloc(0) };
    };
    const client = new SimpleClient(raw);

    for (const kind of Object.values(BINARY_REQUEST_KIND))
      await client.sendBinaryRequest(kind, customCode, payload);

    assert.equal(frames.length, 2);
    assert.deepEqual(frames[0], frames[1]);
  });

  it('forwards a custom code and opaque payload to sendCommand', async () => {
    const customCode = 60_000;
    const payload = Buffer.from([0xAA, 0xBB, 0xCC]);
    const expectedResponse = Buffer.from('opaque response');
    const raw = mockRawClient();
    raw.sendCommand = async (code, sentPayload, options) => {
      assert.equal(code, customCode);
      assert.deepEqual(sentPayload, payload);
      assert.equal(
        options?.rawKind,
        BINARY_REQUEST_KIND.NonReplicated
      );
      return {
        status: 0,
        length: expectedResponse.length,
        data: expectedResponse,
      };
    };
    const client = new SimpleClient(raw);
    const response = await client.sendBinaryRequest(
      BINARY_REQUEST_KIND.NonReplicated,
      customCode,
      payload
    );
    assert.deepEqual(response, expectedResponse);
  });

  it('copies the payload before awaiting the client provider', async () => {
    const payload = Buffer.from([0xAA, 0xBB, 0xCC]);
    const expectedPayload = Buffer.from(payload);
    const raw = mockRawClient();
    raw.sendCommand = async (_code, sentPayload) => {
      assert.deepEqual(sentPayload, expectedPayload);
      return { status: 0, length: 1, data: Buffer.alloc(0) };
    };

    const request = new SimpleClient(raw).sendBinaryRequest(
      BINARY_REQUEST_KIND.NonReplicated,
      60_001,
      payload
    );
    payload.fill(0);

    await request;
  });

  it('normalizes a one-byte response to an empty buffer', async () => {
    const raw = mockRawClient();
    raw.sendCommand = async () => ({
      status: 0,
      length: 1,
      data: Buffer.from([1]),
    });

    const response = await new SimpleClient(raw).sendBinaryRequest(
      BINARY_REQUEST_KIND.NonReplicated,
      COMMAND_CODE.Ping,
      Buffer.alloc(0)
    );

    assert.deepEqual(response, Buffer.alloc(0));
  });

});
