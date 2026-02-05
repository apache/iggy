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

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { uuidv7, uuidv4 } from "uuidv7";
import { SEND_MESSAGES, type SendMessages } from "./send-messages.command.js";
import { HeaderValue, HeaderKeyFactory } from "./header.utils.js";

describe("SendMessages", () => {
  describe("serialize", () => {
    const t1 = {
      streamId: 911,
      topicId: 213,
      messages: [
        { payload: "a" },
        { id: 0, payload: "b" },
        { id: 123, payload: "X" },
        { id: 0n, payload: "c" },
        { id: 1236234534554n, payload: "X" },
        { id: uuidv4(), payload: "d" },
        { id: uuidv7(), payload: "e" },
      ],
    };

    it("serialize SendMessages into a buffer", () => {
      assert.deepEqual(SEND_MESSAGES.serialize(t1).length, 589);
    });

    it("serialize all kinds of messageId", () => {
      assert.doesNotThrow(() => SEND_MESSAGES.serialize(t1));
    });

    it("does not throw on number message id", () => {
      const t = { ...t1, messages: [{ id: 42, payload: "m" }] };
      assert.doesNotThrow(() => SEND_MESSAGES.serialize(t));
    });

    it("does not throw on bigint message id", () => {
      const t = { ...t1, messages: [{ id: 123n, payload: "m" }] };
      assert.doesNotThrow(() => SEND_MESSAGES.serialize(t));
    });

    it("does not throw on uuid message id", () => {
      const t = { ...t1, messages: [{ id: uuidv4(), payload: "uuid" }] };
      assert.doesNotThrow(() => SEND_MESSAGES.serialize(t));
    });

    it("throw on invalid string message id", () => {
      const t = { ...t1, messages: [{ id: "foo", payload: "m" }] };
      assert.throws(() => SEND_MESSAGES.serialize(t));
    });

    it("throw on invalid number message id", () => {
      const t = { ...t1, messages: [{ id: -12, payload: "n" }] };
      assert.throws(() => SEND_MESSAGES.serialize(t));
    });

    it("throw on invalid bigint message id", () => {
      const t = { ...t1, messages: [{ id: -12n, payload: "bn" }] };
      assert.throws(() => SEND_MESSAGES.serialize(t));
    });

    it("serialize message with headers", () => {
      const t: SendMessages = {
        streamId: 911,
        topicId: 213,
        messages: [
          {
            payload: "m",
            headers: [
              {
                key: HeaderKeyFactory.String("p"),
                value: HeaderValue.Bool(true),
              },
            ],
          },
          {
            payload: "q",
            headers: [
              {
                key: HeaderKeyFactory.String("v-aze"),
                value: HeaderValue.Uint8(128),
              },
            ],
          },
          {
            payload: "x",
            headers: [
              {
                key: HeaderKeyFactory.String("q"),
                value: HeaderValue.Double(1 / 3),
              },
            ],
          },
          {
            payload: "s",
            headers: [
              {
                key: HeaderKeyFactory.String("x"),
                value: HeaderValue.Uint32(123),
              },
            ],
          },
          {
            payload: "r",
            headers: [
              {
                key: HeaderKeyFactory.String("y"),
                value: HeaderValue.Uint64(42n),
              },
            ],
          },
          {
            payload: "g",
            headers: [
              {
                key: HeaderKeyFactory.String("y"),
                value: HeaderValue.Float(42.3),
              },
            ],
          },
          {
            payload: "c",
            headers: [
              {
                key: HeaderKeyFactory.String("ID"),
                value: HeaderValue.String(uuidv7()),
              },
            ],
          },
          {
            payload: "l",
            headers: [
              {
                key: HeaderKeyFactory.String("val"),
                value: HeaderValue.Raw(Buffer.from(uuidv4())),
              },
            ],
          },
        ],
      };
      assert.doesNotThrow(() => SEND_MESSAGES.serialize(t));
    });
  });
});
