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

import { COMMAND_CODE } from '../command.code.js';
import { responseError } from '../error.utils.js';
import { PARTITION_CONTEXT_SIZE } from '../vsr/header.js';
import { BATCH_HEADER_SIZE, FRAME_HEADER_SIZE } from './iggy-header.utils.js';

const INVALID_COMMAND = 3;
const INVALID_SIZE_BYTES = 80;
const INVALID_MESSAGES_COUNT = 4009;
const MAX_U32 = 0xFFFF_FFFF;
/** Partition id, current offset and message count, which lead a poll reply. */
const POLL_REPLY_FIELDS_SIZE = 4 + 8 + 4;

export const US_PER_MS = 1_000n;
export const MAX_DEFERRED_POLL_WAIT_US = 600_000_000n;
/** The longest wait, and room for the final read and the reply. */
export const MAX_DEFERRED_POLL_TIMEOUT_US = 630_000_000n;
/** The time that a request which waits leaves between its wait and its timeout, for the final read and the reply. */
export const MIN_DEFERRED_POLL_REPLY_MARGIN_US = 1_000_000n;
/** The reply header, one batch header and one message header: a smaller byte limit can return no message. */
export const MIN_DEFERRED_POLL_MAX_BYTES =
  POLL_REPLY_FIELDS_SIZE + PARTITION_CONTEXT_SIZE + BATCH_HEADER_SIZE + FRAME_HEADER_SIZE;
/** The most partitions that one fetch names. */
export const MAX_PARTITIONS_PER_REQUEST = 1_000;

/** The options of a deferred poll (106), which follow the body of a plain poll (100). */
export const DEFERRED_POLL_OFFSET = {
  wait: 0,
  minCount: 8,
  maxBytes: 12,
  requestTimeout: 16
} as const;
export const DEFERRED_POLL_OPTIONS_SIZE = 8 + 4 + 4 + 8;

/**
 * How a deferred poll waits on the server for messages. Durations are in
 * milliseconds.
 */
export type DeferredPollOptions = {
  /** How long the server waits for `minCount` messages before it replies with what it has. At most 600 s. */
  maxWait: number,
  /** The number of messages that ends the wait early, from 1 to the count of the poll. */
  minCount: number,
  /** The largest reply, with its headers and batch framing. At least 344 bytes. */
  maxBytes: number,
  /**
   * When the whole call gives up, at most 630 s. It must leave 1 s after a
   * wait for the final read and the reply.
   */
  requestTimeout: number
};

/** The defaults of the Rust SDK: wait 1 s for one message, read at most 1 MiB, give up after 11 s. */
export const DEFAULT_DEFERRED_POLL_OPTIONS: Readonly<DeferredPollOptions> = Object.freeze({
  maxWait: 1_000,
  minCount: 1,
  maxBytes: 1_048_576,
  requestTimeout: 11_000
});

/** The shared wait and request timeout options. */
export type ReadinessWaitOptions = Pick<DeferredPollOptions, 'maxWait' | 'requestTimeout'>;

/** Whether the server accepts a wait and a request timeout, in microseconds. */
export const isDeferredBudget = (wait: bigint, timeout: bigint): boolean =>
  wait <= MAX_DEFERRED_POLL_WAIT_US && timeout >= 1n && timeout <= MAX_DEFERRED_POLL_TIMEOUT_US &&
  (wait === 0n || timeout >= wait + MIN_DEFERRED_POLL_REPLY_MARGIN_US);

/**
 * Whether the server accepts a wait and a request timeout of a fetch (109).
 * A fetch needs the reply margin also at a wait of 0, because the server ends
 * its partition reads before the request timeout.
 */
export const isFetchBudget = (wait: bigint, timeout: bigint): boolean =>
  isDeferredBudget(wait, timeout) && timeout >= wait + MIN_DEFERRED_POLL_REPLY_MARGIN_US;

/**
 * `options` over the defaults, checked as the Rust SDK checks them before
 * anything is sent: a minimum count outside 1 to `count` fails with 4009, a
 * wait or a request timeout that the server refuses with 3, and a byte limit
 * that cannot hold one message with 80.
 */
export const deferredPollOptions = (
  options: Partial<DeferredPollOptions>,
  count: number,
  command: number = COMMAND_CODE.PollMessagesDeferred
): DeferredPollOptions => {
  const minCount = options.minCount ?? DEFAULT_DEFERRED_POLL_OPTIONS.minCount;
  if (!Number.isInteger(minCount) || minCount < 1 || minCount > count)
    throw responseError(command, INVALID_MESSAGES_COUNT);
  const budget = readinessWaitOptions(options, command);
  const maxBytes = options.maxBytes ?? DEFAULT_DEFERRED_POLL_OPTIONS.maxBytes;
  if (!Number.isInteger(maxBytes) || maxBytes < MIN_DEFERRED_POLL_MAX_BYTES || maxBytes > MAX_U32)
    throw responseError(command, INVALID_SIZE_BYTES);
  return { ...budget, minCount, maxBytes };
};

/** `options` over the defaults, a wait or a request timeout that the server refuses failing with 3. */
export const readinessWaitOptions = (
  options: Partial<ReadinessWaitOptions>,
  command: number
): ReadinessWaitOptions => {
  const maxWait = options.maxWait ?? DEFAULT_DEFERRED_POLL_OPTIONS.maxWait;
  const requestTimeout = options.requestTimeout ?? DEFAULT_DEFERRED_POLL_OPTIONS.requestTimeout;
  if (!isDuration(maxWait) || !isDuration(requestTimeout) ||
      !isDeferredBudget(BigInt(maxWait) * US_PER_MS, BigInt(requestTimeout) * US_PER_MS))
    throw responseError(command, INVALID_COMMAND);
  return { maxWait, requestTimeout };
};

export const serializeDeferredPollOptions = (options: DeferredPollOptions): Buffer => {
  const b = Buffer.allocUnsafe(DEFERRED_POLL_OPTIONS_SIZE);
  b.writeBigUInt64LE(BigInt(options.maxWait) * US_PER_MS, DEFERRED_POLL_OFFSET.wait);
  b.writeUInt32LE(options.minCount, DEFERRED_POLL_OFFSET.minCount);
  b.writeUInt32LE(options.maxBytes, DEFERRED_POLL_OFFSET.maxBytes);
  b.writeBigUInt64LE(BigInt(options.requestTimeout) * US_PER_MS, DEFERRED_POLL_OFFSET.requestTimeout);
  return b;
};

const isDuration = (milliseconds: number): boolean => Number.isSafeInteger(milliseconds) && milliseconds >= 0;
