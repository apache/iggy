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

import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { u128ToBuf } from "../number.utils.js";
import {
  isValidMessageId,
  serializeMessageId,
  resolveMessageId,
  mintMessageId,
} from "./message.utils.js";

const MESSAGE_ID_SIZE = 16;
const MAX_U128 = (1n << 128n) - 1n;
const NIL_UUID = "00000000-0000-0000-0000-000000000000";
const isZero = (b: Buffer) => b.every((byte) => byte === 0);

describe("isValidMessageId", () => {
  it("accepts undefined, string, number, and bigint", () => {
    assert.ok(isValidMessageId(undefined));
    assert.ok(isValidMessageId("id"));
    assert.ok(isValidMessageId(7));
    assert.ok(isValidMessageId(7n));
  });

  it("rejects other types", () => {
    assert.ok(!isValidMessageId(null));
    assert.ok(!isValidMessageId({}));
  });
});

describe("serializeMessageId", () => {
  it("serializes undefined to a zero u128", () => {
    assert.deepEqual(serializeMessageId(), Buffer.alloc(MESSAGE_ID_SIZE, 0));
  });

  it("serializes a number as little-endian u128", () => {
    assert.deepEqual(serializeMessageId(7), u128ToBuf(7n));
  });

  it("serializes a bigint as little-endian u128", () => {
    assert.deepEqual(serializeMessageId(8n), u128ToBuf(8n));
  });

  it("serializes a UUID string to the same bytes as its numeric value", () => {
    const uuid = "00000000-0000-0000-0000-000000000007";
    assert.deepEqual(serializeMessageId(uuid), u128ToBuf(7n));
  });

  it("accepts the largest u128", () => {
    assert.deepEqual(serializeMessageId(MAX_U128), u128ToBuf(MAX_U128));
  });

  it("rejects a numeric id at or above 2^128", () => {
    assert.throws(() => serializeMessageId(1n << 128n), /2\^128/);
  });

  it("rejects a negative numeric id", () => {
    assert.throws(() => serializeMessageId(-1n), />= 0/);
  });

  it("rejects an unparsable string", () => {
    assert.throws(() => serializeMessageId("not-a-uuid"), /invalid message id/);
  });

  it("rejects an invalid type", () => {
    assert.throws(() => serializeMessageId({}), /invalid message id/);
  });
});

describe("resolveMessageId", () => {
  it("mints a non-zero id for undefined, 0, and 0n", () => {
    for (const id of [undefined, 0, 0n]) {
      const b = resolveMessageId(id);
      assert.equal(b.length, MESSAGE_ID_SIZE);
      assert.ok(!isZero(b));
    }
  });

  it("mints for the all-zero nil UUID string", () => {
    assert.ok(!isZero(resolveMessageId(NIL_UUID)));
  });

  it("passes a provided non-zero id through unchanged", () => {
    assert.deepEqual(resolveMessageId(7n), serializeMessageId(7n));
  });
});

describe("mintMessageId", () => {
  it("returns a non-zero 16-byte buffer", () => {
    const b = mintMessageId();
    assert.equal(b.length, MESSAGE_ID_SIZE);
    assert.ok(!isZero(b));
  });

  it("produces unique, non-zero ids across pool refills", () => {
    const seen = new Set<string>();
    for (let i = 0; i < 10_000; i++) {
      const hex = mintMessageId().toString("hex");
      assert.notEqual(hex, "0".repeat(MESSAGE_ID_SIZE * 2));
      seen.add(hex);
    }
    assert.equal(seen.size, 10_000);
  });

  it("returns owned bytes that survive a later refill", () => {
    const first = mintMessageId().toString("hex");
    const held = mintMessageId();
    const snapshot = held.toString("hex");
    for (let i = 0; i < 10_000; i++) mintMessageId();
    assert.equal(held.toString("hex"), snapshot);
    assert.notEqual(snapshot, first);
  });
});
