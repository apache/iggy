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
import { deserializeTokens } from "./token.utils.js";

// Real server wire shape: [nameLength: u8][name][expiry: 8-byte LE, always present, 0 = never-expiring].
// See core/binary_protocol/.../get_personal_access_tokens.rs and core/server/src/responses.rs:1195-1209.
const tokenRecord = (name: string, expiry: bigint = 0n): Buffer => {
  const nameBuf = Buffer.from(name, "utf-8");
  const head = Buffer.from([nameBuf.length]);
  const expiryBuf = Buffer.alloc(8);
  expiryBuf.writeBigUInt64LE(expiry);
  return Buffer.concat([head, nameBuf, expiryBuf]);
};

describe("deserializeTokens", () => {
  it("reads all tokens in a 3-token buffer, including the one after the 2nd", () => {
    const t1 = tokenRecord("ci-a", 123n);
    const t2 = tokenRecord("ci-b");
    const t3 = tokenRecord("x");
    const buffer = Buffer.concat([t1, t2, t3]);

    const tokens = deserializeTokens(buffer);

    assert.deepEqual(
      tokens.map((t) => t.name),
      ["ci-a", "ci-b", "x"],
    );
    assert.notEqual(tokens[0].expiry, null);
    assert.equal(tokens[1].expiry, null);
    assert.equal(tokens[2].expiry, null);
  });

  it("reads all tokens cleanly regardless of record length ordering", () => {
    const t1 = tokenRecord("short");
    const t2 = tokenRecord("second");
    const t3 = tokenRecord("a-much-longer-token-name-here");
    const t4 = tokenRecord("last");
    const buffer = Buffer.concat([t1, t2, t3, t4]);

    const tokens = deserializeTokens(buffer);

    assert.deepEqual(
      tokens.map((t) => t.name),
      ["short", "second", "a-much-longer-token-name-here", "last"],
    );
    assert.ok(tokens.every((t) => t.expiry === null));
  });

  it("decodes a single token with no trailing data", () => {
    const buffer = tokenRecord("solo");

    const tokens = deserializeTokens(buffer);

    assert.deepEqual(tokens, [{ name: "solo", expiry: null }]);
  });

  it("decodes a non-zero expiry into the correct point in time", () => {
    const expiryMicros = 1700000000000000n;
    const buffer = tokenRecord("with-expiry", expiryMicros);

    const tokens = deserializeTokens(buffer);

    assert.equal(tokens[0].expiry?.getTime(), Number(expiryMicros / 1000n));
  });

  it("throws on a buffer truncated before the expiry field", () => {
    const partial = Buffer.concat([Buffer.from([4]), Buffer.from("iggy")]);

    assert.throws(() => deserializeTokens(partial), RangeError);
  });
});
