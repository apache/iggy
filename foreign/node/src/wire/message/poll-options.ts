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
import { deferredPollOptions, type DeferredPollOptions } from './deferred-poll.utils.js';

const INVALID_CONFIGURATION = 2;
const INVALID_MESSAGES_COUNT = 4009;
/** The request timeout of a read is its wait plus this margin, as in the Rust SDK. */
const REQUEST_TIMEOUT_MARGIN_MS = 10_000;

/** When a message counts for an automatic commit policy. */
export const CommitAfter = {
  /**
   * Once the application asks for the next message or batch, returns from its
   * handler with success, or closes the reader. A handler that fails leaves its
   * message uncounted.
   */
  Processing: 'processing',
  /** When the message is handed over. */
  Delivery: 'delivery',
  /** Only when the application marks its checkpoint, for messages processed concurrently. */
  Marked: 'marked'
} as const;

export type CommitAfter = typeof CommitAfter[keyof typeof CommitAfter];

/** The event on which an automatic commit policy also stores. */
export type CommitEvery =
  | { readonly kind: 'message' }
  | { readonly kind: 'messages', readonly count: number }
  | { readonly kind: 'batch' };

export const CommitEvery = {
  /** Each counted message, or each counted batch of a partition reader. */
  Message: Object.freeze({ kind: 'message' }) as CommitEvery,
  /** The last message of each batch. */
  Batch: Object.freeze({ kind: 'batch' }) as CommitEvery,
  /** This many counted messages of a partition since its last store. */
  Messages: (count: number): CommitEvery => {
    if (!Number.isInteger(count) || count < 1)
      throw new RangeError('the count of messages between stores must be a positive integer');
    return Object.freeze({ kind: 'messages', count });
  }
};

/** The settings of an automatic commit policy. Durations are in milliseconds. */
export type AutoCommit = {
  /** Store the latest counted position at this interval, when it moved. */
  interval?: number,
  /** Also store it on this event. */
  every?: CommitEvery,
  /** When a message counts, `CommitAfter.Processing` by default. */
  after?: CommitAfter
};

/** Who stores the offsets of the messages that a read returns. */
export type CommitPolicy =
  | { readonly kind: 'manual' }
  | { readonly kind: 'onRead' }
  | { readonly kind: 'auto', readonly interval?: number, readonly every?: CommitEvery, readonly after: CommitAfter };

/** Every 10,000 messages or 5 seconds, whichever comes first, after processing. */
export const DEFAULT_AUTO_COMMIT: CommitPolicy = Object.freeze({
  kind: 'auto',
  interval: 5_000,
  every: CommitEvery.Messages(10_000),
  after: CommitAfter.Processing
});

export const CommitPolicy = {
  /** Nothing is stored. The application commits checkpoints. The default. */
  Manual: Object.freeze({ kind: 'manual' }) as CommitPolicy,
  /**
   * The server stores the offset of the last message of each read as part of
   * the read, in one round trip. While the primary stays up a message is
   * delivered at most once. A failover before the store replicates can
   * deliver the last batch again.
   */
  OnRead: Object.freeze({ kind: 'onRead' }) as CommitPolicy,
  /**
   * The client stores the latest counted position, at least once with
   * `CommitAfter.Processing`. A one-shot poll refuses it, a partition reader and
   * a consumer accept it. Without settings it stores every 10,000 messages or
   * 5 seconds after processing.
   */
  Auto: (settings?: AutoCommit): CommitPolicy => settings === undefined ? DEFAULT_AUTO_COMMIT : Object.freeze({
    kind: 'auto',
    ...(settings.interval === undefined ? {} : { interval: settings.interval }),
    ...(settings.every === undefined ? {} : { every: settings.every }),
    after: settings.after ?? CommitAfter.Processing
  })
};

/**
 * The limits of one read and who stores its offsets: a poll of one
 * partition, or a fetch of several partitions of one primary that a reader
 * sends, whose sections share them. Durations are in milliseconds.
 */
export type PollOptions = {
  /** Most messages of one batch, 1000 by default. */
  count: number,
  /**
   * How long the server waits for `minCount` messages, at most 600 s. Zero
   * answers at once and is the default of a poll. The call gives up 10 s later.
   */
  maxWait: number,
  /** How many messages end the wait early, 1 to `count`, 1 by default. */
  minCount: number,
  /**
   * The largest reply, 1 MiB by default. A first message larger than it comes
   * alone in its read.
   */
  maxBytes: number,
  /** Who stores the offsets, `CommitPolicy.Manual` by default. */
  commit: CommitPolicy
};

export const DEFAULT_POLL_OPTIONS: Readonly<PollOptions> = Object.freeze({
  count: 1_000,
  maxWait: 0,
  minCount: 1,
  maxBytes: 1_048_576,
  commit: CommitPolicy.Manual
});

/**
 * `options` over the defaults, checked before anything is sent: a count below
 * 1 fails with 4009, an automatic commit without an interval or an event, or
 * with an interval that is not positive, fails with 2 (InvalidConfiguration),
 * and the wire limits fail as a deferred poll checks them.
 */
export const pollOptions = (
  options: Partial<PollOptions> = {},
  command: number = COMMAND_CODE.PollMessagesDeferred
): PollOptions => {
  const resolved = { ...DEFAULT_POLL_OPTIONS, ...options };
  if (!Number.isInteger(resolved.count) || resolved.count < 1)
    throw responseError(command, INVALID_MESSAGES_COUNT);
  const { commit } = resolved;
  if (commit.kind === 'auto' && (commit.interval === undefined && commit.every === undefined ||
      commit.interval !== undefined && !(commit.interval > 0)))
    throw responseError(command, INVALID_CONFIGURATION, 'an automatic commit needs a positive interval or an event');
  wireOptions(resolved, command);
  return resolved;
};

/** The wire limits of a read with `options`: the request timeout is the wait plus 10 s. */
export const wireOptions = (
  options: PollOptions,
  command: number = COMMAND_CODE.PollMessagesDeferred
): DeferredPollOptions => deferredPollOptions({
  maxWait: options.maxWait,
  minCount: options.minCount,
  maxBytes: options.maxBytes,
  requestTimeout: options.maxWait + REQUEST_TIMEOUT_MARGIN_MS
}, options.count, command);
