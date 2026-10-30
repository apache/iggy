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

import type { CommandResponse } from '../../client/client.type.js';
import { FETCH_PROGRESS_UNCERTAIN, FetchProgressUncertainError } from '../../client/client.operation.js';
import { COMMAND_CODE } from '../command.code.js';
import { wrapCommand } from '../command.utils.js';
import { DeserializeError, responseError } from '../error.utils.js';
import type { ResponseError } from '../error.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';
import type { Consumer } from '../offset/offset.utils.js';
import {
  EMPTY_PARTITION_CONTEXT, PARTITION_CONTEXT_SIZE, deserializePartitionContext, type PartitionContext
} from '../vsr/header.js';
import {
  PollingStrategyKind, deserializeMessages, type PollingStrategy, type PollMessagesResponse
} from './poll.utils.js';
import {
  MAX_PARTITIONS_PER_REQUEST, MIN_DEFERRED_POLL_REPLY_MARGIN_US, US_PER_MS, deferredPollOptions, isFetchBudget,
  type DeferredPollOptions
} from './deferred-poll.utils.js';

const INVALID_COMMAND = 3;
const STATUS_OK = 0;
const SECTIONS_COUNT_SIZE = 4;
/** A split count could loop on or misread the other start points, so the server refuses them. */
const FETCH_STRATEGY_KINDS = new Set<number>([PollingStrategyKind.Offset, PollingStrategyKind.Next]);

/** The fields that follow the consumer, stream and topic of a fetch. */
export const FETCH_MESSAGES_FIELD = {
  wait: 0,
  requestTimeout: 8,
  minCount: 16,
  count: 20,
  maxBytes: 24,
  autoCommit: 28,
  targetsCount: 29
} as const;
export const FETCH_MESSAGES_FIELDS_SIZE = 8 + 8 + 4 + 4 + 4 + 1 + 4;
/** A partition id, the kind and the value of its start point, its byte limit and its fence. */
export const FETCH_TARGET_FIELD = {
  partitionId: 0,
  kind: 4,
  value: 5,
  maxBytes: 13,
  incarnation: 17,
  ownerGeneration: 25
} as const;
export const FETCH_TARGET_SIZE = 4 + 1 + 8 + 4 + 8 + 8;
/** The fields of one section header of a fetch reply. */
export const FETCH_SECTION_FIELD = {
  partitionId: 0,
  status: 4,
  currentOffset: 8,
  messagesCount: 16,
  context: 20,
  nextOffset: 44,
  recordsLength: 52
} as const;
export const FETCH_SECTION_SIZE = 4 + 4 + 8 + 4 + PARTITION_CONTEXT_SIZE + 8 + 4;

/** The section count and one section header per target, which lead a fetch reply. */
export const fetchReplyHeadSize = (targets: number): number => SECTIONS_COUNT_SIZE + targets * FETCH_SECTION_SIZE;

/**
 * `options` over the defaults, checked as for a deferred poll. A fetch also
 * needs a timeout greater than 1 s at a wait of 0, so local setup can leave
 * the server its mandatory 1 s reply margin. Invalid options fail with 3.
 */
export const fetchOptions = (options: Partial<DeferredPollOptions>, count: number): DeferredPollOptions => {
  const command = COMMAND_CODE.FetchMessages;
  const checked = deferredPollOptions(options, count, command);
  const timeout = BigInt(checked.requestTimeout) * US_PER_MS;
  if (timeout <= MIN_DEFERRED_POLL_REPLY_MARGIN_US || !isFetchBudget(BigInt(checked.maxWait) * US_PER_MS, timeout))
    throw responseError(command, INVALID_COMMAND);
  return checked;
};

/** A partition of a fetch and its start point. */
export type FetchTarget = {
  partitionId: number,
  /** Offset or Next. Other start points fail with 3 before anything is sent. */
  pollingStrategy: PollingStrategy,
  /**
   * Context of the section that the offset came from
   * (`FetchMessagesSection.context`), so a fetch does not read that offset
   * in a partition deleted and recreated since. Without one the client takes
   * the incarnation of its cached route.
   */
  context?: PartitionContext
};

/**
 * Parameters of a fetch (command 109): one wait on many partitions of a
 * topic, which then reads them into one reply.
 */
export type FetchMessages = {
  /** Stream identifier */
  streamId: Id,
  /** Topic identifier */
  topicId: Id,
  /** The consumer whose offsets the Next targets and `autocommit` use */
  consumer: Consumer,
  /**
   * 1 to 1000 partitions, each once, with one primary: a group of
   * `RawClient.readinessGroups`. The server reads them in this order, so a
   * caller that moves the partitions it read to the end serves them in turn.
   */
  targets: FetchTarget[],
  /** The most messages of the whole reply */
  count: number,
  /** Whether the server stores the offset of each partition that it reads */
  autocommit: boolean,
  /**
   * The wait over `DEFAULT_DEFERRED_POLL_OPTIONS`. `maxBytes` limits the
   * records of the whole reply and of each partition. Unlike a deferred poll,
   * a fetch with a wait of 0 needs a timeout greater than 1 s so local setup
   * can leave the server its mandatory 1 s reply margin.
   */
  options?: Partial<DeferredPollOptions>,
  /**
   * Cancels the fetch. A sent fetch is cancelled on the server too: one
   * without `autocommit` ends at once, a committing one answers once, with
   * its messages or with nothing stored (rejected with the abort reason).
   * Unknown committing outcomes retain local progress blocks.
   */
  signal?: AbortSignal
};

/** The part of a fetch reply about one partition. */
export type FetchMessagesSection = {
  partitionId: number,
  /**
   * 0 when the partition was read. Else the error code of this partition,
   * and the other fields carry no position. Local status 14010 includes `progressError`
   * and blocks this partition until explicit abandonment. Healthy siblings remain usable.
   */
  status: number,
  /** Current offset in the partition */
  currentOffset: bigint,
  /** Number of messages in `records` */
  messagesCount: number,
  /** Partition incarnation, owner generation and metadata op of the read */
  context: PartitionContext,
  /** Where the next fetch of this partition starts, as an Offset target */
  nextOffset: bigint,
  /** The batch records of the section, as a poll reply carries them, see `decodeFetchSection` */
  records: Buffer,
  /** Explicit local recovery is required for this section. Healthy sibling sections remain usable. */
  progressError?: FetchProgressUncertainError
};

/**
 * Fetch command definition. The reply holds one section per partition that
 * the server read or refused.
 */
export const FETCH_MESSAGES = {
  code: COMMAND_CODE.FetchMessages,

  serialize: ({ streamId, topicId, consumer, targets, count, autocommit, options = {} }: FetchMessages): Buffer => {
    const command = COMMAND_CODE.FetchMessages;
    const { maxWait, minCount, maxBytes, requestTimeout } = fetchOptions(options, count);
    if (targets.length === 0 || targets.length > MAX_PARTITIONS_PER_REQUEST ||
        new Set(targets.map(({ partitionId }) => partitionId)).size !== targets.length ||
        targets.some(({ pollingStrategy }) => !FETCH_STRATEGY_KINDS.has(pollingStrategy.kind)))
      throw responseError(command, INVALID_COMMAND);
    const fields = Buffer.allocUnsafe(FETCH_MESSAGES_FIELDS_SIZE);
    fields.writeBigUInt64LE(BigInt(maxWait) * US_PER_MS, FETCH_MESSAGES_FIELD.wait);
    fields.writeBigUInt64LE(BigInt(requestTimeout) * US_PER_MS, FETCH_MESSAGES_FIELD.requestTimeout);
    fields.writeUInt32LE(minCount, FETCH_MESSAGES_FIELD.minCount);
    fields.writeUInt32LE(count, FETCH_MESSAGES_FIELD.count);
    fields.writeUInt32LE(maxBytes, FETCH_MESSAGES_FIELD.maxBytes);
    fields.writeUInt8(autocommit ? 1 : 0, FETCH_MESSAGES_FIELD.autoCommit);
    fields.writeUInt32LE(targets.length, FETCH_MESSAGES_FIELD.targetsCount);
    const body = Buffer.allocUnsafe(targets.length * FETCH_TARGET_SIZE);
    targets.forEach(({ partitionId, pollingStrategy, context = EMPTY_PARTITION_CONTEXT }, index) => {
      const position = index * FETCH_TARGET_SIZE;
      body.writeUInt32LE(partitionId, position + FETCH_TARGET_FIELD.partitionId);
      body.writeUInt8(pollingStrategy.kind, position + FETCH_TARGET_FIELD.kind);
      body.writeBigUInt64LE(pollingStrategy.value, position + FETCH_TARGET_FIELD.value);
      // The server serves a partition only the bytes that it knows the
      // partition holds, so a smaller limit would only split its batches.
      body.writeUInt32LE(maxBytes, position + FETCH_TARGET_FIELD.maxBytes);
      body.writeBigUInt64LE(context.incarnation, position + FETCH_TARGET_FIELD.incarnation);
      body.writeBigUInt64LE(context.ownerGeneration, position + FETCH_TARGET_FIELD.ownerGeneration);
    });
    return Buffer.concat([
      uint8ToBuf(consumer.kind),
      serializeIdentifier(consumer.id),
      serializeIdentifier(streamId),
      serializeIdentifier(topicId),
      fields,
      body
    ]);
  },

  deserialize: ({ data, fetchProgress }: CommandResponse): FetchMessagesSection[] => {
    const count = data.length >= SECTIONS_COUNT_SIZE ? data.readUInt32LE(0) : -1;
    if (count < 0 || count > MAX_PARTITIONS_PER_REQUEST || data.length < fetchReplyHeadSize(count))
      throw new DeserializeError('fetch reply does not hold its section table');
    let recordsAt = fetchReplyHeadSize(count);
    const sections = Array.from({ length: count }, (_, index): FetchMessagesSection => {
      const position = SECTIONS_COUNT_SIZE + index * FETCH_SECTION_SIZE;
      const status = data.readUInt32LE(position + FETCH_SECTION_FIELD.status);
      const recordsLength = data.readUInt32LE(position + FETCH_SECTION_FIELD.recordsLength);
      if ((status !== STATUS_OK && recordsLength !== 0) || recordsAt + recordsLength > data.length)
        throw new DeserializeError('fetch reply section does not match its records');
      const records = data.subarray(recordsAt, recordsAt + recordsLength);
      recordsAt += recordsLength;
      const partitionId = data.readUInt32LE(position + FETCH_SECTION_FIELD.partitionId);
      const progressError = fetchProgress?.partitions.has(partitionId)
        ? new FetchProgressUncertainError(fetchProgress.operationId, responseError(COMMAND_CODE.FetchMessages, status))
        : undefined;
      return {
        partitionId,
        status: progressError ? FETCH_PROGRESS_UNCERTAIN : status,
        currentOffset: data.readBigUInt64LE(position + FETCH_SECTION_FIELD.currentOffset),
        messagesCount: data.readUInt32LE(position + FETCH_SECTION_FIELD.messagesCount),
        context: deserializePartitionContext(data, position + FETCH_SECTION_FIELD.context),
        nextOffset: data.readBigUInt64LE(position + FETCH_SECTION_FIELD.nextOffset),
        records,
        ...(progressError ? { progressError } : {})
      };
    });
    if (recordsAt !== data.length)
      throw new DeserializeError('fetch reply has bytes after the records of its last section');
    return sections;
  }
};

/** The refusal that a section with an error status carries, naming its partition. */
export const fetchSectionError = ({ partitionId, status, progressError }: FetchMessagesSection): ResponseError | FetchProgressUncertainError => {
  if (progressError) return progressError;
  const error = responseError(COMMAND_CODE.FetchMessages, status, `partition ${partitionId}`);
  error.fromServer = true;
  return error;
};

/**
 * The messages of a section, as a poll of its partition returns them. A
 * section with an error status throws that error.
 */
export const decodeFetchSection = (section: FetchMessagesSection): PollMessagesResponse => {
  if (section.status !== STATUS_OK)
    throw fetchSectionError(section);
  const messages = deserializeMessages(section.records);
  if (messages.length !== section.messagesCount)
    throw new DeserializeError('fetch section count does not match its messages', { record: section.records });
  return {
    partitionId: section.partitionId,
    context: section.context,
    currentOffset: section.currentOffset,
    count: section.messagesCount,
    messages
  };
};

/**
 * Executable fetch function. The call goes to the primary of the targets,
 * and takes one of the deferred request slots of the client while it waits.
 * A refusal as not the primary, or for a history or an owner that changed,
 * of the call or of a section, marks the routes of those partitions stale,
 * so the next `RawClient.readinessGroups` looks them up again. A fetch with
 * `autocommit` or a Next target is never sent again after it reached the
 * server. A lost reply of a fetch with `autocommit` fails with
 * `FetchProgressUncertainError` (14010) and blocks its partitions until
 * `abandonOperation`. A lost reply of any other fetch fails with 57.
 */
export const fetchMessages = wrapCommand(FETCH_MESSAGES, ({ signal }) => ({ signal }));
