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

import { Readable } from "node:stream";
import type { ClientConfigOrString } from "../client/client.type.js";
import type { Id } from '../wire/identifier.utils.js';
import { SimpleClient } from "../client/client.js";
import { normalizeClientConfig } from "../client/client.config.js";
import { debug } from "../client/client.debug.js";
import { getRawClient } from "../client/client.socket.js";
import {
  NotConnectedError, OperationUncertainError, VsrResponseTimeoutError
} from "../client/client.operation.js";
import { ResponseError } from "../wire/error.utils.js";
import {
  NO_ASSIGNED_PARTITION,
  type PollMessages
} from "../wire/message/poll-messages.command.js";
import {
  type PollingStrategy,
  type CommandAPI,
  Consumer
} from "../wire/index.js";

const STALE_CLIENT = 30;
const UNAUTHENTICATED = 40;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;
const NOT_PRIMARY = 89;
const RECOVERED_ERROR_CODES = new Set([
  STALE_CLIENT, UNAUTHENTICATED, TRANSIENT_NOT_COMMITTED, TRANSIENT_NOT_ACCEPTED, NOT_PRIMARY
]);

const wait = (interval: number, cb?: () => void): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(() => resolve(cb ? cb() : undefined), interval)
  });

// The client binds the session again, signs in again, reconnects or routes
// again on its own after these errors, so they must not end a stream that
// nothing restarts.
const pollsAgainAfter = (error: unknown): boolean =>
  error instanceof OperationUncertainError || error instanceof VsrResponseTimeoutError ||
  error instanceof NotConnectedError ||
  (error instanceof ResponseError && RECOVERED_ERROR_CODES.has(error.errorCode));

async function* genEagerUntilPoll(
  c: CommandAPI,
  poll: PollMessages,
  stopped: () => boolean,
  interval = 1000,
  endOnLastOffset = false
) {
  const state: Map<string, number> = new Map();

  while (!stopped()) {
    const r = await c.message.poll(poll).catch((error: unknown) => {
      if (!pollsAgainAfter(error)) throw error;
      debug('consumer stream poll failed, polling again', error);
      return undefined;
    });
    if (!r || (r.count === 0 && r.partitionId === NO_ASSIGNED_PARTITION)) {
      await wait(interval);
      continue;
    }
    yield r;
    state.set(`${r.partitionId}`, r.count);

    if (Array.from(state).every(([, last]) => last === 0)) {
      if( endOnLastOffset )
        return;
      else
        await wait(interval);
    }
  }
};

// A running stream polls again after NotConnectedError, so the factory signs
// in first: a wrong address or broken TLS then rejects the factory call. The
// stream owns this client, so a failed sign-in or setup destroys it.
const openClient = async (
  config: ClientConfigOrString,
  setup?: (c: SimpleClient) => Promise<unknown>
): Promise<SimpleClient> => {
  const options = normalizeClientConfig(config);
  const raw = getRawClient(options);
  try {
    await raw.authenticate(options.credentials);
    const c = new SimpleClient(raw);
    await setup?.(c);
    return c;
  } catch (error) {
    raw.destroy();
    throw error;
  }
};

// The stream owns its client, so a closed stream must not keep the links,
// heartbeat and group membership of that client alive.
const pollStream = (c: SimpleClient, poll: PollMessages, interval: number, endOnLastOffset: boolean) => {
  const stream: Readable = Readable.from(
    genEagerUntilPoll(c, poll, () => stream.destroyed, interval, endOnLastOffset),
    { objectMode: true }
  );
  stream.once('close', () => void c.destroy());
  return stream;
};


export type ConsumerStreamRequest = {
  streamId: Id,
  topicId: Id,
  pollingStrategy: PollingStrategy,
  count: number,
  interval?: number,
  autocommit?: boolean
  endOnLastOffset?: boolean
}

export type SingleConsumerStreamRequest = ConsumerStreamRequest & {
  partitionId: number,
};

export type GroupConsumerStreamRequest = ConsumerStreamRequest & { groupName: string };

export const singleConsumerStream = (config: ClientConfigOrString) => async (
  {
    streamId,
    topicId,
    partitionId,
    pollingStrategy,
    count,
    interval = 1000,
    autocommit = true,
    endOnLastOffset = false
  }: SingleConsumerStreamRequest
): Promise<Readable> => {
  const c = await openClient(config);

  const poll = {
    streamId,
    topicId,
    partitionId,
    consumer: Consumer.Single,
    pollingStrategy,
    count,
    autocommit
  };

  return pollStream(c, poll, interval, endOnLastOffset);
};


export const groupConsumerStream = (config: ClientConfigOrString) =>
  async function groupConsumerStream({
    groupName,
    streamId,
    topicId,
    pollingStrategy,
    count,
    interval = 1000,
    autocommit = true,
    endOnLastOffset = false
  }: GroupConsumerStreamRequest): Promise<Readable> {
    const s = await openClient(config, (client) => client.group.ensureAndJoin(streamId, topicId, groupName));

    const poll = {
      streamId,
      topicId,
      consumer: Consumer.Group(groupName),
      partitionId: null,
      pollingStrategy,
      count,
      autocommit
    };
    return pollStream(s, poll, interval, endOnLastOffset);
  };
