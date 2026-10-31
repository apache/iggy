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

import { responseError } from '../error.utils.js';
import { EMPTY_PARTITION_CONTEXT, type PartitionContext } from '../vsr/header.js';
import {
  PollingStrategyKind, type PollingStrategy, type PollMessagesResponse, type PollingStrategyKindValue
} from './poll.utils.js';

const INVALID_COMMAND = 3;

/** The reader and the partition generation that delivered a message. */
export type ReaderStamp = { readonly reader: number, readonly generation: number };

/**
 * The position after one delivered message. A commit stores its offset, so it
 * covers all earlier messages of the partition. A checkpoint keeps the
 * partition incarnation and owner that delivered the message, so a commit after
 * a change of the partition history fails. Only a read creates one.
 */
export class Checkpoint {
  /** @internal */
  constructor(
    readonly partitionId: number,
    readonly offset: bigint,
    /** @internal The partition history that delivered the message. */
    readonly context: PartitionContext,
    /** @internal The reader and generation that delivered it, none for a one-shot poll. */
    readonly stamp?: ReaderStamp
  ) {}
}

/**
 * A polling strategy bound to the partition and history of the batch it
 * continues. A read of another partition refuses it.
 */
export type Continuation = PollingStrategy & {
  readonly partitionId: number,
  readonly context: PartitionContext
};

/** A continuation of `partitionId` under `context`. */
export const continuation = (
  partitionId: number, kind: PollingStrategyKindValue, value: bigint, context: PartitionContext
): Continuation => Object.freeze({ kind, value, partitionId, context }) as Continuation;

/** Refuses a continuation of another partition than `partitionId` with 3 (InvalidCommand). */
export const validatePartition = (strategy: PollingStrategy, partitionId: number, command: number): void => {
  const bound = (strategy as Partial<Continuation>).partitionId;
  if (bound !== undefined && bound !== partitionId)
    throw responseError(command, INVALID_COMMAND,
      `a continuation of partition ${bound} cannot read partition ${partitionId}`);
};

/** A batch of one partition with its checkpoints and its continuation. */
export type PolledBatch = PollMessagesResponse & {
  /** The position after the last message, to commit the whole batch. Undefined for an empty batch. */
  checkpoint(): Checkpoint | undefined,
  /** The position after `messages[index]`, to commit a processed part of the batch. */
  checkpointAt(index: number): Checkpoint | undefined,
  /**
   * The position after the batch, bound to its partition and history. After
   * the last message it is the next offset. An empty batch keeps the start of
   * its read: First becomes offset 0 and Next stays Next.
   */
  continuation(): Continuation
};

const sameContext = (left: PartitionContext, right: PartitionContext): boolean =>
  left.incarnation === right.incarnation && left.ownerGeneration === right.ownerGeneration &&
  left.metadataOp === right.metadataOp;

/** `polled` with the checkpoints and the continuation of a read that started at `start`. */
export const polledBatch = (polled: PollMessagesResponse, start: PollingStrategy, stamp?: ReaderStamp): PolledBatch => ({
  ...polled,
  checkpointAt(index: number) {
    const message = polled.messages[index];
    return message === undefined ? undefined
      : new Checkpoint(polled.partitionId, message.headers.offset, polled.context, stamp);
  },
  checkpoint() {
    return this.checkpointAt(polled.messages.length - 1);
  },
  continuation() {
    const last = polled.messages.at(-1);
    if (last !== undefined)
      return continuation(polled.partitionId, PollingStrategyKind.Offset, last.headers.offset + 1n, polled.context);
    // A reply without a history names no partition, so it continues as its read started.
    if (sameContext(polled.context, EMPTY_PARTITION_CONTEXT))
      return start as Continuation;
    return start.kind === PollingStrategyKind.First
      ? continuation(polled.partitionId, PollingStrategyKind.Offset, 0n, polled.context)
      : continuation(polled.partitionId, start.kind, start.value, polled.context);
  }
});
