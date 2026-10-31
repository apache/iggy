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
import type { ClientProvider } from '../client/client.type.js';
import type { Id } from '../wire/identifier.utils.js';
import { ConsumerKind } from '../wire/offset/offset.utils.js';
import { ensureConsumerGroup } from '../wire/consumer-group/ensure-group.virtual.command.js';
import type { Message, PollingStrategy } from '../wire/message/poll.utils.js';
import type { PollOptions } from '../wire/message/poll-options.js';
import type { Checkpoint, PolledBatch } from '../wire/message/checkpoint.js';
import { PartitionReader, ReadError } from './partition-reader.js';

/** One message that a consumer hands over. */
export type ConsumerMessage = {
  partitionId: number,
  message: Message,
  /** The position after this message, to commit it with `IggyConsumer.commit`. */
  checkpoint: Checkpoint
};

/** Parameters of `client.consumer`: a standalone consumer of the given partitions. */
export type ConsumerRequest = {
  /** The consumer identity whose offsets the consumer reads and stores. */
  name: Id,
  streamId: Id,
  topicId: Id,
  partitions: number[],
  /** Where a partition without a position starts, Next by default. */
  start?: PollingStrategy,
  /** The limits of each read and the commit policy. A read waits up to 5 s by default. */
  options?: Partial<PollOptions>,
  /** Gets each read error that the reader retries by itself, while the consumer goes on. */
  onError?: (error: ReadError) => void
};

/** Parameters of `client.consumerGroup`: a member of a consumer group, created when it is missing. */
export type ConsumerGroupRequest = Omit<ConsumerRequest, 'partitions'>;

/**
 * Hands over the messages of a topic one at a time, on a `PartitionReader`.
 * With `CommitPolicy.Manual` the application commits checkpoints. With
 * `CommitPolicy.OnRead` the server stores the offset of each read and
 * `shutdown()` moves it back to the last message handed over. With
 * `CommitPolicy.Auto` a message counts when it is handed over
 * (`after: delivery`), once the iterator is read again or the handler of
 * `consume()` returns with success (`after: processing`), or on `mark()`,
 * and the latest counted position is stored at the interval or event of the
 * policy and on shutdown. A read error that the reader retries by itself goes
 * to `onError` and the consumer goes on. A message that does not decode ends
 * the iteration with its `ReadError`, whose `skip` passes it with `seek`.
 */
export class IggyConsumer implements AsyncIterable<ConsumerMessage> {
  private uncounted?: { checkpoint: Checkpoint, endsBatch: boolean };
  private stopped = false;
  /** The batch whose messages the consumer hands over, until a seek of its partition drops the rest. */
  private current?: PolledBatch;

  /** @internal */
  constructor(
    private readonly reader: PartitionReader,
    private readonly after?: string,
    private readonly onError?: (error: ReadError) => void
  ) {
    reader.countsMessages = true;
  }

  /** The partitions that the consumer reads now. */
  partitions(): number[] {
    return this.reader.partitions();
  }

  async *[Symbol.asyncIterator](): AsyncIterator<ConsumerMessage> {
    while (!this.stopped) {
      const batch = await this.nextBatch();
      if (!batch) return;
      this.current = batch;
      for (let index = 0; index < batch.messages.length && this.current === batch; index += 1) {
        if (this.stopped) return;
        const message = this.hand(batch, index);
        yield message;
        this.countProcessed();
      }
    }
  }

  /**
   * Hands each message to `handler` until the consumer shuts down or a handler
   * fails, which ends the loop with its error and leaves its message uncounted.
   * A message counts only once its handler returns, so a shutdown while the
   * handler runs leaves it uncounted too.
   */
  async consume(handler: (message: ConsumerMessage) => Promise<void> | void): Promise<void> {
    while (!this.stopped) {
      const batch = await this.nextBatch();
      if (!batch) return;
      this.current = batch;
      for (let index = 0; index < batch.messages.length && this.current === batch; index += 1) {
        if (this.stopped) return;
        const message = this.hand(batch, index);
        // A shutdown while the handler runs must not count its message.
        const handled = this.uncounted;
        this.uncounted = undefined;
        await handler(message);
        if (handled) this.reader.countDelivered(handled.checkpoint, 1, handled.endsBatch);
      }
    }
  }

  /** Stores the offset of a checkpoint that this consumer handed over. */
  commit(checkpoint: Checkpoint): Promise<void> {
    return this.reader.commit(checkpoint);
  }

  /** Counts a checkpoint under `CommitPolicy.Auto` with `after: marked`. */
  mark(checkpoint: Checkpoint): void {
    this.reader.mark(checkpoint);
  }

  /**
   * Sets the next read position of a partition, as `PartitionReader.seek`
   * does, and drops the rest of the batch of that partition that the consumer
   * hands over.
   */
  async seek(partitionId: number, start: PollingStrategy): Promise<void> {
    await this.reader.seek(partitionId, start);
    if (this.current?.partitionId === partitionId) this.current = undefined;
  }

  /**
   * Stops the consumer: counts the last message under `after: processing`,
   * closes the reader, which stores or rewinds the offsets, and leaves the group.
   * A later call waits for the first one and ends as it does.
   */
  async shutdown(): Promise<void> {
    if (!this.stopped) {
      this.stopped = true;
      this.countProcessed();
    }
    await this.reader.close();
  }

  /**
   * The messages as an object mode `Readable`, which shuts the consumer down
   * when it is destroyed. It takes the next message only when its own consumer
   * asks, so a message counts as processed only after that.
   */
  toReadable(): Readable {
    const messages = this[Symbol.asyncIterator]();
    const shutdown = () => this.shutdown();
    return new Readable({
      objectMode: true,
      highWaterMark: 0,
      read() {
        messages.next().then(({ done, value }) => {
          if (!this.destroyed) this.push(done ? null : value);
        }, (error: unknown) => {
          if (!this.destroyed) this.destroy(error as Error);
        });
      },
      destroy(error, callback) {
        shutdown().catch(() => undefined).finally(() => callback(error));
      }
    });
  }

  private async nextBatch(): Promise<PolledBatch | undefined> {
    while (true) {
      try {
        return await this.reader.next();
      } catch (error) {
        if (this.stopped && !(error instanceof ReadError)) return undefined;
        if (!(error instanceof ReadError) || error.skip) throw error;
        this.onError?.(error);
      }
    }
  }

  private hand(batch: PolledBatch, index: number): ConsumerMessage {
    const checkpoint = batch.checkpointAt(index)!;
    const endsBatch = index === batch.messages.length - 1;
    this.reader.delivered(checkpoint);
    if (this.after === 'delivery') this.reader.countDelivered(checkpoint, 1, endsBatch);
    else if (this.after === 'processing') this.uncounted = { checkpoint, endsBatch };
    return { partitionId: batch.partitionId, message: batch.messages[index], checkpoint };
  }

  private countProcessed(): void {
    const uncounted = this.uncounted;
    this.uncounted = undefined;
    if (uncounted) this.reader.countDelivered(uncounted.checkpoint, 1, uncounted.endsBatch);
  }
}

const afterOf = (options?: Partial<PollOptions>): string | undefined =>
  options?.commit?.kind === 'auto' ? options.commit.after : undefined;

/** Opens a standalone consumer of `partitions` under the consumer identity `name`. */
export const openConsumer = (getClient: ClientProvider) =>
  async ({ name, streamId, topicId, partitions, start, options, onError }: ConsumerRequest): Promise<IggyConsumer> =>
    new IggyConsumer(await PartitionReader.open(getClient, {
      streamId, topicId, consumer: { kind: ConsumerKind.Single, id: name }, partitions, start, options
    }), afterOf(options), onError);

/** Opens a member of the consumer group `name`, created when it is missing. */
export const openConsumerGroup = (getClient: ClientProvider) =>
  async ({ name, streamId, topicId, start, options, onError }: ConsumerGroupRequest): Promise<IggyConsumer> => {
    await ensureConsumerGroup(getClient)(streamId, topicId, String(name));
    return new IggyConsumer(await PartitionReader.open(getClient, {
      streamId, topicId, consumer: { kind: ConsumerKind.Group, id: name }, start, options
    }), afterOf(options), onError);
  };
