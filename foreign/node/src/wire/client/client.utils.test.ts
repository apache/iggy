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
import { deserializeClient } from "./client.utils.js";

const buildClientRecord = (
  clientId: number,
  userId: number,
  transport: number,
  address: string,
  consumerGroupCount: number
): Buffer => {
  const addressBuf = Buffer.from(address, "utf-8");
  const buf = Buffer.alloc(13 + addressBuf.length + 4);
  buf.writeUInt32LE(clientId, 0);
  buf.writeUInt32LE(userId, 4);
  buf.writeUInt8(transport, 8);
  buf.writeUInt32LE(addressBuf.length, 9);
  addressBuf.copy(buf, 13);
  buf.writeUInt32LE(consumerGroupCount, 13 + addressBuf.length);
  return buf;
};

describe("deserializeClient", () => {
  it("deserializes valid client data with tcp transport", () => {
    const record = buildClientRecord(1, 42, 1, "127.0.0.1:8090", 3);
    const { bytesRead, data } = deserializeClient(record);

    assert.equal(bytesRead, record.length);
    assert.equal(data.clientId, 1);
    assert.equal(data.userId, 42);
    assert.equal(data.transport, "tcp");
    assert.equal(data.address, "127.0.0.1:8090");
    assert.equal(data.consumerGroupCount, 3);
  });

  it("deserializes valid client data with quic transport", () => {
    const record = buildClientRecord(2, 99, 2, "10.0.0.1:9090", 0);
    const { data } = deserializeClient(record);

    assert.equal(data.clientId, 2);
    assert.equal(data.userId, 99);
    assert.equal(data.transport, "quic");
    assert.equal(data.address, "10.0.0.1:9090");
    assert.equal(data.consumerGroupCount, 0);
  });

  it("deserializes from a non-zero buffer offset", () => {
    const padding = Buffer.from([0xaa, 0xbb, 0xcc, 0xdd]);
    const record = buildClientRecord(5, 10, 1, "localhost:1234", 1);
    const combined = Buffer.concat([padding, record]);

    const { bytesRead, data } = deserializeClient(combined, padding.length);
    assert.equal(bytesRead, record.length);
    assert.equal(data.clientId, 5);
    assert.equal(data.userId, 10);
    assert.equal(data.address, "localhost:1234");
    assert.equal(data.consumerGroupCount, 1);
  });

  it("handles unknown transport gracefully", () => {
    const record = buildClientRecord(3, 1, 99, "127.0.0.1:80", 0);
    const { data } = deserializeClient(record);
    assert.equal(data.transport, "unknown_transport_99");
  });

  it("throws when total buffer length is less than minimum 17 bytes", () => {
    const shortBuf = Buffer.alloc(16);
    assert.throws(() => deserializeClient(shortBuf), {
      message: "Client does not exist",
    });
  });

  it("throws when buffer is >= 17 bytes but remaining bytes at pos < 17", () => {
    const buf = Buffer.alloc(20);
    assert.throws(() => deserializeClient(buf, 10), {
      message: "Client does not exist",
    });
  });

  it("throws when address payload is truncated", () => {
    const record = buildClientRecord(1, 1, 1, "127.0.0.1:8090", 1);
    // Truncate the last 5 bytes (cuts into consumerGroupCount)
    const truncated = record.subarray(0, record.length - 5);
    assert.throws(() => deserializeClient(truncated), {
      message: "Client does not exist",
    });
  });
});
