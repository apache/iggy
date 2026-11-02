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

import { Readable } from 'node:stream';
import type { ClientConfigOrString } from '../client/client.type.js';
import type { Id } from '../wire/identifier.utils.js';
import { SimpleClient } from '../client/client.js';
import { normalizeClientConfig } from '../client/client.config.js';
import { ConnectionClosedError } from '../client/client.connection.js';
import { debug } from '../client/client.debug.js';
import {
  FetchProgressUncertainError, LaneBlockedError, NotConnectedError, OperationUncertainError, VsrResponseTimeoutError
} from '../client/client.operation.js';
import { getRawClient } from '../client/client.socket.js';
import { ResponseError } from '../wire/error.utils.js';
import { Consumer } from '../wire/offset/offset.utils.js';
import type { PollingStrategy } from '../wire/message/poll.utils.js';
import type { DeferredPollOptions } from '../wire/message/deferred-poll.utils.js';
import { CommitPolicy, pollOptions, type PollOptions } from '../wire/message/poll-options.js';
import type { PolledBatch } from '../wire/message/checkpoint.js';
import { ReadError, type PartitionReader } from './partition-reader.js';

const DEFAULT_POLLING_RETRY_INTERVAL_MS = 1_000;
// A closing stream keeps its client this long for the rewind and the leave of
// its reader. A membership that it could not leave ends with the session lease.
const CLOSE_TIMEOUT_MS = 5_000;
const STALE_CLIENT = 30;
const UNAUTHENTICATED = 40;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const NOT_PRIMARY = 89;
const DEFERRED_POLL_CAPACITY_EXCEEDED = 90;
const RECOVERED_ERROR_CODES = new Set([
  STALE_CLIENT, UNAUTHENTICATED, TRANSIENT_NOT_COMMITTED, TRANSIENT_NOT_ACCEPTED, NOT_PRIMARY,
  DEFERRED_POLL_CAPACITY_EXCEEDED
]);

// The client binds the session again, reconnects or routes again on its own
// after these errors, and the reader reads again, so they do not end a stream
// that nothing restarts. A message that does not decode and a block that
// needs explicit recovery end it.
const readsAgainAfter = (error: unknown): boolean => {
  if (error instanceof ReadError && error.skip) return false;
  const cause = error instanceof ReadError ? error.error : error;
  if (cause instanceof FetchProgressUncertainError || (cause instanceof LaneBlockedError && cause.requiresExplicitRecovery))
    return false;
  return cause instanceof OperationUncertainError || cause instanceof VsrResponseTimeoutError ||
    cause instanceof NotConnectedError || cause instanceof ConnectionClosedError ||
    (cause instanceof ResponseError && RECOVERED_ERROR_CODES.has(cause.errorCode));
};

const isUnauthenticated = (error: unknown): boolean => {
  const cause = error instanceof ReadError ? error.error : error;
  return cause instanceof ResponseError && cause.errorCode === UNAUTHENTICATED;
};

export type ConsumerStreamRequest = {
  streamId: Id,
  topicId: Id,
  pollingStrategy: PollingStrategy,
  count: number,
  /**
   * How each read waits on the server for messages: `maxWait`, `minCount`
   * and `maxBytes` of `PollOptions`. A read waits up to 5 s by default.
   */
  pollOptions?: Partial<DeferredPollOptions>,
  /** The pause in milliseconds before a refused partition reads again. Defaults to 1000. */
  pollingRetryInterval?: number,
  /** Whether the server stores the offset of each read (`CommitPolicy.OnRead`). Defaults to true. */
  autocommit?: boolean
  /** Ends the stream once the last read of every partition found no message. */
  endOnLastOffset?: boolean
}

export type SingleConsumerStreamRequest = ConsumerStreamRequest & {
  partitionId: number,
};

export type GroupConsumerStreamRequest = ConsumerStreamRequest & { groupName: string };

/** The reader options of a stream request. */
const readerOptions = ({ count, pollOptions: wait = {}, autocommit = true }: ConsumerStreamRequest):
  Partial<PollOptions> => {
  const options: Partial<PollOptions> = {
    count,
    ...(wait.maxWait === undefined ? {} : { maxWait: wait.maxWait }),
    ...(wait.minCount === undefined ? {} : { minCount: wait.minCount }),
    ...(wait.maxBytes === undefined ? {} : { maxBytes: wait.maxBytes }),
    commit: autocommit ? CommitPolicy.OnRead : CommitPolicy.Manual
  };
  // Invalid options fail here, before the stream owns a client.
  pollOptions({ maxWait: 5_000, ...options });
  return options;
};

/** A `Readable` of the batches of a reader on a client of its own, which it closes with the stream. */
const readerStream = async (
  config: ClientConfigOrString,
  open: (client: SimpleClient) => Promise<PartitionReader>,
  endOnLastOffset: boolean,
  committing: boolean
): Promise<Readable> => {
  const options = normalizeClientConfig(config);
  const raw = getRawClient(options);
  let reader: PartitionReader;
  try {
    await raw.authenticate(options.credentials);
    reader = await open(new SimpleClient(raw));
  } catch (error) {
    raw.destroy();
    throw error;
  }
  // A 40 also comes from a sign-in that the server refuses, as for an
  // inactive user, so a refused sign-in ends the stream.
  const signInAgain = async (error: unknown): Promise<void> => {
    if (!isUnauthenticated(error)) return;
    await raw.authenticate(options.credentials).catch((failure: unknown) => {
      if (isUnauthenticated(failure) || !readsAgainAfter(failure)) throw failure;
      debug('consumer stream sign-in failed, reading again', failure);
    });
  };
  // The last batches pushed, so a destroy can take back those still in the buffer.
  const pushed: PolledBatch[] = [];
  const read = async (stream: Readable): Promise<void> => {
    while (!stream.destroyed) {
      try {
        const batch = endOnLastOffset ? await reader.nextUntilEnd() : await reader.next();
        if (stream.destroyed) {
          if (batch) reader.returned(batch);
          return;
        }
        if (batch) {
          pushed.push(batch);
          if (pushed.length > 2) pushed.shift();
        }
        stream.push(batch ?? null);
        return;
      } catch (error) {
        if (stream.destroyed) return;
        if (!readsAgainAfter(error)) throw error;
        debug('consumer stream read failed, reading again', error);
        await signInAgain(error);
      }
    }
  };
  // A destroy closes the reader, which ends a read that waits for messages. Each
  // batch in the buffer of a stream that commits on read is stored already, so
  // that stream reads only when its consumer asks for more.
  return new Readable({
    objectMode: true,
    highWaterMark: committing ? 0 : 1,
    read() {
      read(this).catch((error: unknown) => {
        if (!this.destroyed) this.destroy(error as Error);
      });
    },
    destroy(error, callback) {
      let released = false;
      const release = () => {
        if (released) return;
        released = true;
        raw.destroy();
      };
      for (const batch of pushed.slice(pushed.length - this.readableLength).reverse()) reader.returned(batch);
      // A client without a session can neither rewind nor leave, so it goes at once.
      if (!raw.isAuthenticated) release();
      const timer = setTimeout(release, CLOSE_TIMEOUT_MS);
      reader.close().catch(() => undefined).finally(() => {
        clearTimeout(timer);
        release();
        callback(error);
      });
    }
  });
};

/**
 * A `Readable` of the batches of one partition, read by a `PartitionReader`
 * of a standalone consumer on a client of its own. Prefer
 * `client.consumer`, which hands over one message at a time on your client.
 */
export const singleConsumerStream = (config: ClientConfigOrString) =>
  async (request: SingleConsumerStreamRequest): Promise<Readable> => {
    const options = readerOptions(request);
    return readerStream(config, (client) => client.message.reader({
      streamId: request.streamId, topicId: request.topicId, consumer: Consumer.Single,
      partitions: [request.partitionId], start: request.pollingStrategy, options,
      retryInterval: request.pollingRetryInterval ?? DEFAULT_POLLING_RETRY_INTERVAL_MS
    }), request.endOnLastOffset ?? false, options.commit === CommitPolicy.OnRead);
  };

/**
 * A `Readable` of the batches of a consumer group member, read by a
 * `PartitionReader` on a client of its own, which creates the group when it is
 * missing. Prefer `client.consumerGroup`.
 */
export const groupConsumerStream = (config: ClientConfigOrString) =>
  async (request: GroupConsumerStreamRequest): Promise<Readable> => {
    const options = readerOptions(request);
    return readerStream(config, async (client) => {
      await client.group.ensure(request.streamId, request.topicId, request.groupName);
      return client.message.reader({
        streamId: request.streamId, topicId: request.topicId, consumer: Consumer.Group(request.groupName),
        start: request.pollingStrategy, options,
        retryInterval: request.pollingRetryInterval ?? DEFAULT_POLLING_RETRY_INTERVAL_MS
      });
    }, request.endOnLastOffset ?? false, options.commit === CommitPolicy.OnRead);
  };
