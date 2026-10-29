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

import type { Stream } from '../wire/stream/stream.utils.js';
import type { Topic } from '../wire/topic/topic.utils.js';
import type {
  CreateMessage, Message, MetadataChangeCursor, MetadataChangeGroup, MetadataChangeRecord,
  MetadataCurrentItem, OptionSpec, PollMessagesResponse
} from 'src/wire/index.js';
import type { Client } from '../client/index.js';

/** How a call that a step started in the background ended, and when. */
export type Outcome<T> = { ended: number } & ({ ok: true, value: T } | { ok: false, error: unknown });

/** A call that a step started in the background, with the controller that cancels it. */
export type BackgroundCall<T> = { started: number, cancel: AbortController, outcome: Promise<Outcome<T>> };

export interface TestWorld {
  client: Client
  stream: Stream,
  topic: Topic,
  sendMessages: CreateMessage[],
  polledMessages: Message[],
  rawResponse?: Buffer,
  rawError?: Error,
  /** Key the last stream was created with, absent for a keyless stream */
  streamKey?: Buffer,
  updateError?: unknown,
  optionSpecs?: OptionSpec[],
  userId?: number,
  callError?: Error,
  /** Groups the metadata change steps read */
  changeGroups?: MetadataChangeGroup[],
  /** Cursor the metadata change steps follow from */
  changeCursor?: MetadataChangeCursor,
  /** Records of the last stream that the follow step returned */
  followedChanges?: MetadataChangeRecord[],
  /** Items of the last current state scan */
  currentItems?: MetadataCurrentItem[],
  /** Consumer group that this client joined on the topic */
  groupName?: string,
  lastSentPartition?: number,
  /** Deferred poll that a step started in the background */
  deferredPoll?: BackgroundCall<PollMessagesResponse>,
  /** Fetch that a step started in the background */
  deferredFetch?: BackgroundCall<PollMessagesResponse[]>
};
