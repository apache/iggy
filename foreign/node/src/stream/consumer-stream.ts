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
import type { ClientConfigOrString, RawClient } from "../client/client.type.js";
import type { Id } from '../wire/identifier.utils.js';
import { SimpleClient } from "../client/client.js";
import { normalizeClientConfig } from "../client/client.config.js";
import { debug } from "../client/client.debug.js";
import { getRawClient } from "../client/client.socket.js";
import {
  NotConnectedError, OperationUncertainError, VsrResponseTimeoutError
} from "../client/client.operation.js";
import { ConnectionClosedError } from '../client/client.connection.js';
import { ResponseError } from "../wire/error.utils.js";
import { LEAVE_GROUP } from "../wire/consumer-group/leave-group.command.js";
import { ConsumerKind } from "../wire/offset/offset.utils.js";
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
// A closed group stream keeps its client until the leave ends. A leave that
// misses this deadline leaves the membership to its lease.
const LEAVE_GROUP_TIMEOUT_MS = 5_000;

const wait = (interval: number, cb?: () => void): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(() => resolve(cb ? cb() : undefined), interval)
  });

// The client binds the session again, signs in again, reconnects or routes
// again on its own after these errors, so they must not end a stream that
// nothing restarts.
const pollsAgainAfter = (error: unknown): boolean =>
  error instanceof OperationUncertainError || error instanceof VsrResponseTimeoutError ||
  error instanceof NotConnectedError || error instanceof ConnectionClosedError ||
  (error instanceof ResponseError && RECOVERED_ERROR_CODES.has(error.errorCode));

const isUnauthenticated = (error: unknown): boolean =>
  error instanceof ResponseError && error.errorCode === UNAUTHENTICATED;

async function* genEagerUntilPoll(
  c: CommandAPI,
  poll: PollMessages,
  signIn: () => Promise<unknown>,
  stopped: () => boolean,
  interval = 1000,
  endOnLastOffset = false
) {
  const state: Map<string, number> = new Map();

  while (!stopped()) {
    const r = await c.message.poll(poll).catch(async (error: unknown) => {
      if (!pollsAgainAfter(error)) throw error;
      debug('consumer stream poll failed, polling again', error);
      // A 40 also comes from a sign-in that the server refuses, as for an
      // inactive user, and every later sign-in gets the same refusal. So after
      // a 40 the stream waits for the next sign-in, and a refused one ends it.
      if (isUnauthenticated(error))
        await signIn().catch((failure: unknown) => {
          if (isUnauthenticated(failure) || !pollsAgainAfter(failure)) throw failure;
          debug('consumer stream sign-in failed, polling again', failure);
        });
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

/** The client that a stream owns, and the sign-in that the stream waits for. */
type StreamClient = { client: SimpleClient, raw: RawClient, signIn: () => Promise<boolean> };

// A running stream polls again after NotConnectedError, so the factory signs
// in first: a wrong address or broken TLS then rejects the factory call. The
// stream owns this client, so a failed sign-in or setup destroys it.
const openClient = async (
  config: ClientConfigOrString,
  setup?: (c: SimpleClient) => Promise<unknown>
): Promise<StreamClient> => {
  const options = normalizeClientConfig(config);
  const raw = getRawClient(options);
  const signIn = () => raw.authenticate(options.credentials);
  try {
    await signIn();
    const client = new SimpleClient(raw);
    await setup?.(client);
    return { client, raw, signIn };
  } catch (error) {
    raw.destroy();
    throw error;
  }
};

// A client without a session would have to sign in again to leave, and that
// new session is not the member.
const leaveConsumerGroup = async (raw: RawClient, { streamId, topicId, consumer }: PollMessages): Promise<void> => {
  if (consumer.kind !== ConsumerKind.Group || !raw.isAuthenticated) return;
  await raw.sendCommand(LEAVE_GROUP.code, LEAVE_GROUP.serialize({ streamId, topicId, groupId: consumer.id }),
    { deadline: Date.now() + LEAVE_GROUP_TIMEOUT_MS });
};

// The stream owns its client, so a closed stream must not keep the links,
// heartbeat and group membership of that client alive.
const pollStream = (
  { client, raw, signIn }: StreamClient, poll: PollMessages, interval: number, endOnLastOffset: boolean
) => {
  const stream: Readable = Readable.from(
    genEagerUntilPoll(client, poll, signIn, () => stream.destroyed, interval, endOnLastOffset),
    { objectMode: true }
  );
  // A destroyed stream closes only after its running poll ends, and a poll
  // can wait for a sign-in until its deadline. A client without a session has
  // no membership to leave, so it goes at once, also when it loses its
  // session later.
  const closeGenerator = stream._destroy.bind(stream);
  stream._destroy = (error, callback) => {
    const releaseSignedOut = () => { if (!raw.isAuthenticated) raw.destroy(); };
    releaseSignedOut();
    raw.on('sessionReset', releaseSignedOut);
    closeGenerator(error, callback);
  };
  stream.once('close', () => {
    void leaveConsumerGroup(raw, poll)
      .catch((error: unknown) => { debug('consumer stream group leave failed', error); })
      .finally(() => raw.destroy());
  });
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
