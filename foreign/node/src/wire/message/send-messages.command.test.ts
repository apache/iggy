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


import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { uuidv7, uuidv4 } from 'uuidv7'
import { SEND_MESSAGES, type SendMessages } from './send-messages.command.js';
import type { MessageIdKind } from './message.utils.js';
import { HeaderValue } from './header.utils.js';

describe('SendMessages', () => {

  describe('serialize', () => {

    const t1 = {
      streamId: 911,
      topicId: 213,
      messages: [
        { payload: 'a' },
        { id: 0 as const, payload: 'b' },
        { id: 0n as const, payload: 'c' },
        { id: uuidv4(), payload: 'd' },
        { id: uuidv7(), payload: 'e' },
      ],
    };

    it('serialize SendMessages into a buffer', () => {
      assert.deepEqual(
        SEND_MESSAGES.serialize(t1).length,
        387
      );
    });

    it('serialize all kinds of messageId', () => {
      assert.doesNotThrow(
        () => SEND_MESSAGES.serialize(t1),
      );
    });

    
    it('does not throw on number message id', () => {
      const t = { ...t1, messages: [{ id: 42 as MessageIdKind, payload: 'm' }] };
      assert.doesNotThrow(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('does not throw on bigint message id', () => {
      const t = { ...t1, messages: [{ id: 123n as MessageIdKind, payload: 'm' }] };
      assert.doesNotThrow(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('throw on invalid string message id', () => {
      const t = { ...t1, messages: [{ id: 'foo', payload: 'm' }] };
      assert.throws(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('throw on invalid number message id', () => {
      const t = { ...t1, messages: [{ id: -12, payload: 'n' }] };
      assert.throws(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    it('throw on invalid bigint message id', () => {
      const t = { ...t1, messages: [{ id: -12n, payload: 'bn' }] };
      assert.throws(
        () => SEND_MESSAGES.serialize(t)
      );
    });

    
    it('serialize message with headers', () => {
      const t: SendMessages = {
        streamId: 911,
        topicId: 213,
        messages: [
          { payload: 'm', headers: { p: HeaderValue.Bool(true) } },
          { payload: 'q', headers: { 'v-aze': HeaderValue.Uint8(128) } },
          { payload: 'x', headers: { q: HeaderValue.Double(1/3) } },
          { payload: 's', headers: { x: HeaderValue.Uint32(123) } },
          { payload: 'r', headers: { y: HeaderValue.Uint64(42n) } },
          { payload: 'g', headers: { y: HeaderValue.Float(42.3) } },
          { payload: 'c', headers: { ID: HeaderValue.String(uuidv7()) } },
          { payload: 'l', headers: { val: HeaderValue.Raw(Buffer.from(uuidv4())) } }
        ]
      };
      assert.doesNotThrow(
        () => SEND_MESSAGES.serialize(t)
      );
    });



  });
});
