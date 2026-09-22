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
import { u128ToBuf } from "./number.utils.js";

const MAX_U128 = (1n << 128n) - 1n;
const hex = (v: bigint) => u128ToBuf(v).toString("hex");

describe("u128ToBuf", () => {
  it("encodes zero as 16 zero bytes", () => {
    assert.equal(hex(0n), "0".repeat(32));
  });

  it("encodes a small value little-endian", () => {
    assert.equal(hex(7n), "07" + "0".repeat(30));
  });

  it("orders all 16 bytes little-endian", () => {
    // Distinct bytes so a byte-order slip is visible.
    assert.equal(
      hex(0x0102030405060708090a0b0c0d0e0f10n),
      "100f0e0d0c0b0a090807060504030201",
    );
  });

  it("spans the 64-bit half boundary", () => {
    assert.equal(hex((1n << 64n) - 1n), "ff".repeat(8) + "00".repeat(8));
    assert.equal(hex(1n << 64n), "00".repeat(8) + "01" + "00".repeat(7));
  });

  it("encodes the largest u128 as all ones", () => {
    assert.equal(hex(MAX_U128), "ff".repeat(16));
  });

  it("round-trips through readBigUInt64LE halves", () => {
    for (const v of [0n, 1n, 42n, 1n << 64n, MAX_U128]) {
      const b = u128ToBuf(v);
      const low = b.readBigUInt64LE(0);
      const high = b.readBigUInt64LE(8);
      assert.equal((high << 64n) | low, v);
    }
  });

  it("throws for a value at or above 2^128", () => {
    assert.throws(() => u128ToBuf(1n << 128n));
  });

  it("throws for a negative value", () => {
    assert.throws(() => u128ToBuf(-1n));
  });
});
