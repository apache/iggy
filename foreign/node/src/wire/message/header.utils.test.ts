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
import { uuidv7, uuidv4 } from "uuidv7";
import {
  serializeHeaders,
  deserializeHeaders,
  HeaderValue,
  HeaderKeyFactory,
} from "./header.utils.js";
import { HeaderKind } from "./header.type.js";
import { serializeSendMessages } from "./message.utils.js";

describe("Headers", () => {
  const headers = [
    { key: HeaderKeyFactory.String("p"), value: HeaderValue.Bool(true) },
    { key: HeaderKeyFactory.String("x"), value: HeaderValue.Uint32(123) },
    { key: HeaderKeyFactory.String("y"), value: HeaderValue.Uint64(42n) },
    {
      key: HeaderKeyFactory.String("z"),
      value: HeaderValue.Float(42.20000076293945),
    },
    { key: HeaderKeyFactory.String("a"), value: HeaderValue.Double(1 / 3) },
    { key: HeaderKeyFactory.String("ID"), value: HeaderValue.String(uuidv7()) },
    {
      key: HeaderKeyFactory.String("val"),
      value: HeaderValue.Raw(Buffer.from(uuidv4())),
    },
  ];

  it("serialize/deserialize string keys", () => {
    const s = serializeHeaders(headers);
    const d = deserializeHeaders(s);
    assert.equal(d.length, headers.length);
    for (let i = 0; i < headers.length; i++) {
      assert.equal(d[i].key.kind, headers[i].key.kind);
      assert.equal(d[i].value.kind, headers[i].value.kind);
    }
  });

  it("serialize/deserialize int32 key", () => {
    const int32Headers = [
      { key: HeaderKeyFactory.Int32(42), value: HeaderValue.String("test") },
    ];
    const s = serializeHeaders(int32Headers);
    const d = deserializeHeaders(s);
    assert.equal(d.length, 1);
    assert.equal(d[0].key.kind, HeaderKind.Int32);
    assert.equal(d[0].key.value, 42);
    assert.equal(d[0].value.kind, HeaderKind.String);
    assert.equal(d[0].value.value, "test");
  });
});


describe("header encoding validation", () => {
  const key = HeaderKeyFactory.String("key");
  const value = HeaderValue.String("value");
  const invalid = [
    { name: "empty string key", key: HeaderKeyFactory.String(""), value },
    { name: "empty raw key", key: HeaderKeyFactory.Raw(Buffer.alloc(0)), value },
    { name: "256-byte string key", key: HeaderKeyFactory.String("a".repeat(256)), value },
    { name: "256-byte raw key", key: HeaderKeyFactory.Raw(Buffer.alloc(256)), value },
    { name: "256-byte UTF-8 key", key: HeaderKeyFactory.String("é".repeat(128)), value },
    { name: "empty string value", key, value: HeaderValue.String("") },
    { name: "empty raw value", key, value: HeaderValue.Raw(Buffer.alloc(0)) },
    { name: "256-byte string value", key, value: HeaderValue.String("a".repeat(256)) },
    { name: "256-byte raw value", key, value: HeaderValue.Raw(Buffer.alloc(256)) },
    { name: "256-byte UTF-8 value", key, value: HeaderValue.String("é".repeat(128)) },
    ...[0, 1, 15, 17].flatMap((length) => [
      { name: `${length}-byte Int128`, key, value: HeaderValue.Int128(Buffer.alloc(length)) },
      { name: `${length}-byte Uint128`, key, value: HeaderValue.Uint128(Buffer.alloc(length)) },
    ]),
  ];

  for (const { name, ...header } of invalid) {
    it(`rejects ${name} before encoding a message batch`, () => {
      assert.throws(() => serializeHeaders([header]), /Invalid header/);
      assert.throws(() => serializeSendMessages(1, 1, [
        { id: 1n, payload: "test", headers: [header] },
      ]), /Invalid header/);
    });
  }

  it("round-trips valid byte-length boundaries and fixed-width values", () => {
    const headers = [
      { key: HeaderKeyFactory.Raw(Buffer.from([1])), value: HeaderValue.Raw(Buffer.from([1])) },
      { key: HeaderKeyFactory.Raw(Buffer.alloc(255)), value: HeaderValue.Raw(Buffer.alloc(255)) },
      { key: HeaderKeyFactory.String("a".repeat(255)), value: HeaderValue.String("a".repeat(255)) },
      { key: HeaderKeyFactory.String("é".repeat(127) + "a"), value: HeaderValue.String("é".repeat(127) + "a") },
      { key: HeaderKeyFactory.Int32(-1), value: HeaderValue.Int128(Buffer.alloc(16, 255)) },
      { key, value: HeaderValue.Uint128(Buffer.alloc(16, 255)) },
    ];
    assert.deepEqual(deserializeHeaders(serializeHeaders(headers)), headers);
    assert.ok(serializeSendMessages(1, 1, [{ id: 1n, payload: "test", headers }]).length > 0);
  });

  it("preserves the existing wire encoding of a valid header", () => {
    assert.equal(serializeHeaders([{ key, value }]).toString("hex"),
      "02030000006b6579020500000076616c7565");
  });

  it("allows a message without user headers", () => {
    assert.deepEqual(serializeHeaders(), Buffer.alloc(0));
    assert.deepEqual(serializeHeaders([]), Buffer.alloc(0));
    assert.ok(serializeSendMessages(1, 1, [{ id: 1n, payload: "test" }]).length > 0);
  });
});
