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


import { Readable } from "node:stream";
import type { ClientConfig } from "../client/client.type.js";
import type { Id } from '../wire/identifier.utils.js';
import { getClient } from "../client/client.js";
import { type PollMessages } from "../wire/message/poll-messages.command.js";
import {
  type PollingStrategy,
  type CommandAPI,
  Consumer
} from "../wire/index.js";


const wait = (interval: number, cb?: () => void): Promise<void> =>
  new Promise((resolve) => {
    setTimeout(() => resolve(cb ? cb() : undefined), interval)
  });

async function* genEagerUntilPoll(
  c: CommandAPI,
  poll: PollMessages,
  interval = 1000,
  endOnLastOffset = false
) {
  const state: Map<string, number> = new Map();

  while (true) {
    const r = await c.message.poll(poll);
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

export const singleConsumerStream = (config: ClientConfig) => async (
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
  const c = await getClient(config);

  const poll = {
    streamId,
    topicId,
    partitionId,
    consumer: Consumer.Single,
    pollingStrategy,
    count,
    autocommit
  };

  const ps = Readable.from(
    genEagerUntilPoll(c, poll, interval, endOnLastOffset),
    { objectMode: true }
  );
  return ps;
};


export const groupConsumerStream = (config: ClientConfig) =>
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
    const s = await getClient(config);
    await s.group.ensureAndJoin(streamId, topicId, groupName);

    const poll = {
      streamId,
      topicId,
      consumer: Consumer.Group(groupName),
      partitionId: null,
      pollingStrategy,
      count,
      autocommit
    };
    const ps = Readable.from(
      genEagerUntilPoll(s, poll, interval, endOnLastOffset),
      { objectMode: true }
    );

    return ps;
  };
