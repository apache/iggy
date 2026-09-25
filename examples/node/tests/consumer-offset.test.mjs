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
import { setTimeout as realSetTimeout } from 'node:timers';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import * as sdk from 'apache-iggy';
import debug from 'debug';

const EXAMPLES = [
  'getting-started', 'basic', 'tcp-tls',
  'message-envelope', 'message-headers', 'multi-tenant',
];
const MESSAGE_COUNT = 50;
const EXPECTED_POLLS = 5;
const EXAMPLE_POLL_INTERVAL_MS = 500;
const TEST_TIMEOUT_MS = 10_000;
const FIRST_OFFSETS = [0n, 25n, BigInt(Number.MAX_SAFE_INTEGER) + 1n, null];

for (const example of EXAMPLES) {
  for (const firstOffset of FIRST_OFFSETS) {
    test(`${example}: ${firstOffset === null ? 'empty partition' : `retained messages from ${firstOffset}`}`, { timeout: TEST_TIMEOUT_MS }, async (context) => {
      const servedOffsets = [];
      let pollCount = 0;
      const errors = [];
      const expectedOffsets = Array.from(
        { length: firstOffset === null ? 0 : MESSAGE_COUNT }, (_, index) => firstOffset + BigInt(index),
      );
      const order = JSON.stringify({ orderId: 'order-1', timestamp: 1 });
      const messages = expectedOffsets.map((offset) => ({
        headers: { offset, timestamp: new Date(0) },
        payload: Buffer.from(JSON.stringify(example === 'message-headers'
          ? { messageType: 'OrderConfirmed', data: order }
          : { message_type: 'OrderConfirmed', payload: order })),
        userHeaders: [],
      }));
      const stream = { id: 0, name: 'tenant-1-stream-test' };
      const topic = { id: 0, name: 'test-topic', partitions: [{ id: 0 }] };
      const finished = Promise.withResolvers();
      class TestClient {
        stream = {
          list: async () => [stream], create: async () => stream,
          delete: async () => true,
        };
        topic = {
          list: async () => [topic], create: async () => topic,
          delete: async () => true,
        };
        session = { login: async () => ({ userId: 0 }) };
        message = {
          poll: async ({ pollingStrategy, count }) => {
            pollCount++;
            const batch = messages.filter(
              (message) => message.headers.offset >= pollingStrategy.value,
            ).slice(0, count);
            servedOffsets.push(...batch.map((message) => message.headers.offset));
            return { partitionId: 0, currentOffset: expectedOffsets.at(-1), count: batch.length, messages: batch };
          },
        };
        async destroy() { finished.resolve(); }
      }
      const originalArgv = process.argv;
      const originalDebug = debug.disable();
      const listeners = process.listeners('unhandledRejection');
      context.after(() => {
        process.argv = originalArgv;
        debug.enable(originalDebug);
        for (const listener of process.listeners('unhandledRejection')) {
          if (!listeners.includes(listener)) process.removeListener('unhandledRejection', listener);
        }
      });
      context.mock.method(console, 'log', () => {});
      context.mock.method(console, 'table', () => {});
      context.mock.method(debug, 'log', (...args) => {
        const line = args.join(' ');
        if (/Error|unknown message type/.test(line)) errors.push(line);
      });
      debug.enable('iggy:examples*');
      context.mock.method(globalThis, 'setTimeout', (callback, delay, ...args) =>
        realSetTimeout(callback, delay === EXAMPLE_POLL_INTERVAL_MS ? 0 : delay, ...args));
      context.mock.module('apache-iggy', { namedExports: { ...sdk, Client: TestClient } });
      const entry = new URL(`../src/${example}/consumer.ts?offset=${firstOffset}`, import.meta.url);
      process.argv = [process.execPath, fileURLToPath(entry) + entry.search];
      await import(entry.href);
      await finished.promise;
      assert.deepEqual(errors, [], 'the example must process the payload without handler errors');
      assert.deepEqual(servedOffsets, expectedOffsets, 'the next poll must skip every message already handled');
      assert.equal(pollCount, EXPECTED_POLLS, 'each attempt counts once, including an empty poll');
    });
  }
}
