/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { Readable, pipeline, PassThrough } from "node:stream";
import type { ClientConfig, RawClient } from "../client/client.type.js";
import type { Id } from '../wire/identifier.utils.js';
import { SimpleClient, rawClientGetter } from "../client/client.js";
import { type PollMessages, POLL_MESSAGES } from "../wire/message/poll-messages.command.js";
import { type PollingStrategy, ConsumerKind, type CommandAPI } from "../wire/index.js";


const wait = (interval: number, cb?: () => void): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(() => resolve(cb ? cb() : undefined), interval)
  });

async function* genAutoCommitedPoll(
  c: CommandAPI,
  poll: PollMessages,
  interval = 1000
) {
  const state: Map<string, number> = new Map();

  while (true) {
    const r = await c.message.poll(poll);
    yield r;

    const k = `${r.partitionId}`;
    let part = state.get(k) || 0;    
    part = r.messageCount;
    state.set(k, part);

    if (Array.from(state).every(([, last]) => last === 0)) {
      await wait(interval);
    }
  }
};

async function* genPoll(
  c: RawClient,
  poll: PollMessages,
) {
  const pl = POLL_MESSAGES.serialize(poll);
  yield await c.sendCommand(POLL_MESSAGES.code, pl, false);
};



export const singleConsumerStream = (config: ClientConfig) => async (
  poll: PollMessages,
  // interval: 1000
): Promise<Readable> => {
  const c = await rawClientGetter(config);
  if (!c.isAuthenticated)
    await c.authenticate(config.credentials);
  const ps = Readable.from(genPoll(c, poll), { objectMode: true });
  // const ps = Readable.from(genAutoCommitedPoll(c, poll, interval), { objectMode: true });
  
  return pipeline(
    ps,
    new PassThrough({ objectMode: true }),
    (err) => console.error('pipeline error', err)
  );
};

export type ConsumerGroupStreamRequest = {
  groupId: number,
  streamId: Id,
  topicId: Id,
  pollingStrategy: PollingStrategy,
  count: number,
  interval?: number,
  autocommit?: boolean
}

export const groupConsumerStream = (config: ClientConfig) =>
  async function groupConsumerStream({
    groupId,
    streamId,
    topicId,
    pollingStrategy,
    count,
    interval = 1000,
    autocommit = true
  }: ConsumerGroupStreamRequest): Promise<Readable> {
    const c = await rawClientGetter(config);
    const s = new SimpleClient(c);
    if (!c.isAuthenticated)
      await c.authenticate(config.credentials);

    try {
      await s.group.get({ streamId, topicId, groupId })
      // eslint-disable-next-line @typescript-eslint/no-unused-vars
    } catch (err) {
      await s.group.create({ streamId, topicId, groupId, name: `auto-${groupId}` })
    }

    await s.group.join({ streamId, topicId, groupId });

    const poll = {
      streamId,
      topicId,
      consumer: { kind: ConsumerKind.Group, id: groupId },
      partitionId: 0,
      pollingStrategy,
      count,
      autocommit
    }
    const ps = Readable.from(genAutoCommitedPoll(s, poll, interval), { objectMode: true });
    return ps;
  };

