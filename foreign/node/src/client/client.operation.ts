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

import { HEADER_SIZE, REQUEST_OFFSET, type PartitionContext } from '../wire/vsr/header.js';
import type { VsrSession } from '../wire/vsr/index.js';
import { responseError } from '../wire/error.utils.js';
import { COMMAND_CODE } from '../wire/command.code.js';

const INVALID_MESSAGE_LENGTH = 80;
const TRANSIENT_NOT_ACCEPTED = 58;
export const OPERATION_TIMEOUT_MS = 30_000;
export const FETCH_PROGRESS_UNCERTAIN = 14010;

export class OperationUncertainError extends Error {
  constructor(readonly operationId: bigint | undefined, cause: unknown) {
    super('The operation outcome is unknown.', { cause });
    this.name = 'OperationUncertainError';
  }
}

/** A local recovery key, never a replayable server receipt. */
export class FetchProgressUncertainError extends OperationUncertainError {
  readonly errorCode = FETCH_PROGRESS_UNCERTAIN;
  constructor(operationId: bigint, cause?: unknown) {
    super(operationId, cause);
    this.name = 'FetchProgressUncertainError';
    this.message = `Committing fetch ${operationId} has an unknown outcome; explicitly abandon it to release its progress blocks.`;
  }
}

export class LaneBlockedError extends Error {
  constructor(readonly operationId: bigint, readonly partitionId?: number, readonly requiresExplicitRecovery = false) {
    super(requiresExplicitRecovery
      ? `Committing fetch ${operationId} blocks partition ${partitionId} until explicit abandonment.`
      : 'A retained operation blocks this mutation lane.');
    this.name = 'LaneBlockedError';
  }
}

/**
 * The client has neither the operation nor a proved outcome for it, so the
 * outcome can still be unknown. A session reset or an abandon came before a
 * replay proved the outcome, or the proved outcome aged out.
 */
export class OperationNotRetainedError extends Error {
  constructor(readonly operationId: bigint) {
    super('The operation is not retained; its outcome may still be unknown.');
    this.name = 'OperationNotRetainedError';
  }
}

export class VsrResponseTimeoutError extends Error {
  constructor(timeout: number) {
    super(`timed out after ${timeout} ms waiting for VSR response`);
    this.name = 'VsrResponseTimeoutError';
  }
}

/**
 * Every replica link used its retry budget, or no endpoint accepted a dial.
 * The client connects again on its own: each heartbeat restarts a link with a
 * fresh budget, and the next call dials again.
 */
export class NotConnectedError extends Error {
  constructor(cause?: unknown) {
    super('The client is not connected to any replica.', { cause });
    this.name = 'NotConnectedError';
  }
}

export const waitWithin = async <T>(pending: Promise<T>, deadline: number, signal?: AbortSignal): Promise<T> => {
  void pending.catch(() => undefined);
  signal?.throwIfAborted();
  const remaining = deadline - Date.now();
  if (remaining <= 0)
    throw new VsrResponseTimeoutError(0);
  let timer: NodeJS.Timeout | undefined;
  let abort: (() => void) | undefined;
  try {
    return await Promise.race([pending, new Promise<never>((_resolve, reject) => {
      if (remaining <= 2_147_483_647)
        timer = setTimeout(() => reject(new VsrResponseTimeoutError(remaining)), remaining);
      abort = () => reject(signal?.reason ?? new Error('operation aborted'));
      signal?.addEventListener('abort', abort, { once: true });
    })]);
  } finally {
    clearTimeout(timer);
    if (abort)
      signal?.removeEventListener('abort', abort);
  }
};

export const pause = (milliseconds: number, signal?: AbortSignal, background = false): Promise<void> => {
  signal?.throwIfAborted();
  return new Promise<void>((resolve, reject) => {
    const cleanup = () => signal?.removeEventListener('abort', abort);
    const timer = setTimeout(() => { cleanup(); resolve(); }, milliseconds);
    if (background)
      timer.unref();
    const abort = () => { clearTimeout(timer); cleanup(); reject(signal?.reason); };
    signal?.addEventListener('abort', abort, { once: true });
  });
};

export class ChangeSignal {
  private resolve!: () => void;
  promise: Promise<void> = this.next();

  private next(): Promise<void> {
    return new Promise<void>((resolve) => { this.resolve = resolve; });
  }

  pulse(): void {
    const resolve = this.resolve;
    this.promise = this.next();
    resolve();
  }
}

export class PermitPool {
  private active = 0;
  private readonly waiting: { grant: () => void, granted: boolean }[] = [];

  constructor(private readonly capacity: number) {}

  async acquire(deadline: number, signal: AbortSignal): Promise<() => void> {
    signal.throwIfAborted();
    if (this.active < this.capacity && this.waiting.length === 0) {
      this.active += 1;
      return () => this.release();
    }
    let grant!: () => void;
    const pending = new Promise<void>((resolve) => { grant = resolve; });
    const waiter = { grant, granted: false };
    this.waiting.push(waiter);
    try {
      await waitWithin(pending, deadline, signal);
      return () => this.release();
    } catch (error) {
      if (waiter.granted) this.release();
      else this.waiting.splice(this.waiting.indexOf(waiter), 1);
      throw error;
    }
  }

  private release(): void {
    const waiter = this.waiting.shift();
    if (waiter) {
      waiter.granted = true;
      waiter.grant();
    } else this.active -= 1;
  }
}

/** Writer aliases pin physical bytes after logical completion or abandonment. */
export class FrozenFrame {
  private references = 1;
  private ownerReleased = false;
  constructor(private bytes: Buffer | undefined, private readonly released: () => void) {}

  get buffer(): Buffer {
    if (!this.bytes)
      throw new Error('request frame was released');
    return this.bytes;
  }

  retain(): void {
    if (!this.bytes)
      throw new Error('request frame was released');
    this.references += 1;
  }

  release(): void {
    this.references -= 1;
    if (this.references === 0) {
      this.bytes?.fill(0);
      this.bytes = undefined;
      this.released();
    }
  }

  releaseOwner(): void {
    if (!this.ownerReleased) {
      this.ownerReleased = true;
      this.release();
    }
  }
}

export type ClientOperation = {
  readonly lane: string,
  /** The progress lanes that an offset store or delete, or a progress fetch, holds as well. */
  readonly heldLanes: readonly string[],
  readonly command: number,
  context: PartitionContext,
  frame: FrozenFrame,
  id: bigint,
  readonly retainable: boolean,
  readonly lifetime: AbortController,
  resolving: boolean,
  retained: boolean,
  uncertain: boolean,
  /** An abandon frees the slot of a running replay before the replay ends. */
  holdsSlot: boolean,
  readonly fetchProgress?: FetchProgress
};

/** Only identities and budget remain after an uncertain fetch; no transport or frame survives with them. */
type FetchProgress = {
  id: bigint,
  readonly targets: ReadonlyMap<string, number>,
  readonly size: number,
  retained: boolean
};

type AdmissionWaiter = {
  size: number, retainable: boolean, changed: ChangeSignal,
  previous?: AdmissionWaiter, next?: AdmissionWaiter
};
type LaneWaiters = { head?: AdmissionWaiter, tail?: AdmissionWaiter };

const lanesOf = (operation: ClientOperation): string[] => [operation.lane, ...operation.heldLanes];

export class MutationLanes {
  private readonly lanes = new Map<string, ClientOperation>();
  /** The progress lanes that calls hold while they wait for their next lane. */
  private readonly held = new Set<string>();
  private readonly operations = new Map<bigint, ClientOperation>();
  private readonly fetchProgress = new Map<bigint, FetchProgress>();
  private readonly blockedProgress = new Map<string, FetchProgress>();
  private fetchBytes = 0;
  /**
   * The outcomes that replays proved, oldest first: the committed rejection,
   * or `undefined` for a committed success. A reset keeps them, because
   * operation ids never repeat within a client.
   */
  private readonly proved = new Map<bigint, unknown>();
  private readonly waiting = new Map<string, LaneWaiters>();
  private readonly capacityWaiting = new Set<AdmissionWaiter>();
  private readonly changed = new ChangeSignal();
  private count = 0;
  private bytes = 0;

  constructor(private readonly maxOperations: number, private readonly maxBytes: number) {}

  /**
   * Admits a call to `lane` in arrival order. It takes `heldLanes` first, in
   * their order, and keeps each one while it waits for the next. An offset
   * store or delete holds its progress lane, so a send can pass it while it
   * waits behind a parked poll. A progress fetch holds the progress lane of
   * each target, in ascending partition order, so two fetches never wait for
   * each other.
   */
  async enter(lane: string, command: number, body: Buffer, context: PartitionContext,
    retainable: boolean, session: VsrSession, deadline: number,
    signal: AbortSignal, heldLanes: readonly string[] = [], partitions?: ReadonlyMap<string, number>,
    commits = false): Promise<ClientOperation> {
    const size = retainable ? HEADER_SIZE + body.length : 0;
    const fetchSize = commits ? HEADER_SIZE + body.length : 0;
    if (!Number.isSafeInteger(size) || size > this.maxBytes || fetchSize > this.maxBytes)
      throw responseError(command, INVALID_MESSAGE_LENGTH, 'request exceeds the mutation byte limit');
    const admission: AdmissionWaiter = { size, retainable, changed: new ChangeSignal() };
    // A held lane needs no capacity, so its waiter asks for none and wakes when the lane frees.
    const waiterFor = (step: number): AdmissionWaiter =>
      step < heldLanes.length ? { size: 0, retainable: false, changed: new ChangeSignal() } : admission;
    let step = 0;
    let waiting = waiterFor(step);
    let current = heldLanes[step] ?? lane;
    let queue = this.join(current, waiting);
    // Only the call that took a held lane frees it. A call that waits for a
    // lane holds nothing, so its end must not free the lane.
    const taken: string[] = [];
    try {
      for (;;) {
        signal.throwIfAborted();
        if (Date.now() >= deadline)
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        this.throwIfRetained(current, partitions?.get(current), command === COMMAND_CODE.GetOffset);
        const head = !this.busy(current) && queue.head === waiting;
        if (head && step < heldLanes.length) {
          this.held.add(current);
          taken.push(current);
          this.leave(current, queue, waiting);
          step += 1;
          current = heldLanes[step] ?? lane;
          waiting = waiterFor(step);
          queue = this.join(current, waiting);
          continue;
        }
        if (head &&
            (!retainable || this.count < this.maxOperations) && size <= this.maxBytes - this.bytes) {
          if (commits && (this.fetchProgress.size >= this.maxOperations || fetchSize > this.maxBytes - this.fetchBytes))
            throw responseError(command, TRANSIENT_NOT_ACCEPTED, 'committing fetch recovery capacity is full');
          if (retainable) this.count += 1;
          this.bytes += size;
          let bytes: Buffer;
          try {
            bytes = session.encode(command, body, context);
          } catch (error) {
            if (retainable) this.count -= 1;
            this.bytes -= size;
            throw error;
          }
          const operation: ClientOperation = {
            lane, heldLanes: [...taken], command, context, retainable,
            id: bytes.readBigUInt64LE(REQUEST_OFFSET.request),
            frame: this.freeze(bytes, size),
            lifetime: new AbortController(), resolving: false, retained: false, uncertain: false,
            holdsSlot: retainable,
            fetchProgress: commits ? {
              id: bytes.readBigUInt64LE(REQUEST_OFFSET.request), targets: new Map(partitions), size: fetchSize, retained: false
            } : undefined
          };
          if (operation.fetchProgress) {
            this.fetchProgress.set(operation.id, operation.fetchProgress);
            this.fetchBytes += fetchSize;
          }
          this.lanes.set(lane, operation);
          for (const held of taken.splice(0)) {
            this.held.delete(held);
            this.lanes.set(held, operation);
          }
          this.operations.set(operation.id, operation);
          return operation;
        }
        if (head) this.capacityWaiting.add(waiting);
        await waitWithin(waiting.changed.promise, deadline, signal);
      }
    } catch (error) {
      if (!signal.aborted && error instanceof VsrResponseTimeoutError)
        throw responseError(command, TRANSIENT_NOT_ACCEPTED);
      throw error;
    } finally {
      for (const held of taken) {
        this.held.delete(held);
        this.wakeLane(held);
      }
      this.leave(current, queue, waiting);
    }
  }

  async restart(operation: ClientOperation, body: Buffer, context: PartitionContext, session: VsrSession,
    deadline: number, signal: AbortSignal): Promise<void> {
    operation.frame.releaseOwner();
    const size = operation.retainable ? HEADER_SIZE + body.length : 0;
    for (;;) {
      signal.throwIfAborted();
      if (!this.owns(operation)) throw new OperationNotRetainedError(operation.id);
      if (Date.now() >= deadline) throw new VsrResponseTimeoutError(0);
      if (size <= this.maxBytes - this.bytes) break;
      await waitWithin(this.changed.promise, deadline, signal);
    }
    const bytes = session.encode(operation.command, body, context);
    this.bytes += size;
    this.operations.delete(operation.id);
    if (operation.fetchProgress) {
      this.releaseFetch(operation.fetchProgress);
      operation.fetchProgress.id = bytes.readBigUInt64LE(REQUEST_OFFSET.request);
      this.fetchProgress.set(operation.fetchProgress.id, operation.fetchProgress);
      this.fetchBytes += operation.fetchProgress.size;
    }
    operation.id = bytes.readBigUInt64LE(REQUEST_OFFSET.request);
    operation.context = context;
    operation.frame = this.freeze(bytes, size);
    this.operations.set(operation.id, operation);
  }

  private freeze(bytes: Buffer, size: number): FrozenFrame {
    return new FrozenFrame(bytes, () => {
      this.bytes -= size;
      this.wakeWaiting();
      this.changed.pulse();
    });
  }

  throwIfRetained(lane: string, partitionId?: number, inspection = false): void {
    const fetch = inspection ? undefined : this.blockedProgress.get(lane);
    if (fetch) throw new LaneBlockedError(fetch.id, fetch.targets.get(lane), true);
    const operation = this.lanes.get(lane);
    if (operation?.retained)
      throw new LaneBlockedError(operation.id, partitionId);
  }

  retained(id: bigint): ClientOperation {
    if (this.fetchProgress.get(id)?.retained) throw new FetchProgressUncertainError(id);
    const operation = this.operations.get(id);
    if (!operation?.retained)
      throw new OperationNotRetainedError(id);
    return operation;
  }

  owns(operation: ClientOperation): boolean {
    return this.operations.get(operation.id) === operation;
  }

  retain(operation: ClientOperation): void {
    // A replay keeps the state it has, which an abandon can have dropped.
    if (operation.retainable && this.owns(operation) && !operation.resolving) {
      operation.retained = true;
      for (const lane of lanesOf(operation))
        for (let waiter = this.waiting.get(lane)?.head; waiter; waiter = waiter.next)
          waiter.changed.pulse();
      this.changed.pulse();
    }
  }

  retainFetch(operation: ClientOperation, partitions?: ReadonlySet<number>): void {
    const fetch = operation.fetchProgress;
    if (!fetch || this.fetchProgress.get(fetch.id) !== fetch) return;
    fetch.retained = true;
    for (const [lane, partition] of fetch.targets)
      if (!partitions || partitions.has(partition)) {
        this.blockedProgress.set(lane, fetch);
        for (let waiter = this.waiting.get(lane)?.head; waiter; waiter = waiter.next) waiter.changed.pulse();
      }
  }

  /** Completes a call that proved its outcome. A replay keeps the outcome for a later resolve. */
  prove(operation: ClientOperation, rejection?: unknown): void {
    if (operation.resolving) {
      if (this.proved.size >= this.maxOperations)
        this.proved.delete(this.proved.keys().next().value!);
      this.proved.set(operation.id, rejection);
    }
    this.complete(operation);
  }

  /** Returns or throws the outcome that a replay proved for `id`. */
  provedOutcome(id: bigint): void {
    if (!this.proved.has(id))
      throw new OperationNotRetainedError(id);
    const rejection = this.proved.get(id);
    if (rejection !== undefined)
      throw rejection;
  }

  /**
   * Drops the retained operation `id`, whose outcome stays unknown, and frees
   * its slot. A running replay keeps the lane until it ends, so the lane stays
   * FIFO.
   */
  abandon(id: bigint): void {
    const fetch = this.fetchProgress.get(id);
    if (fetch?.retained) {
      this.releaseFetch(fetch);
      return;
    }
    const operation = this.retained(id);
    operation.retained = false;
    if (!operation.resolving) {
      this.complete(operation);
      return;
    }
    this.releaseSlot(operation);
    this.wakeWaiting();
  }

  complete(operation: ClientOperation): void {
    if (!this.owns(operation))
      return;
    this.operations.delete(operation.id);
    for (const lane of lanesOf(operation)) this.lanes.delete(lane);
    this.releaseSlot(operation);
    if (operation.fetchProgress && !operation.fetchProgress.retained) this.releaseFetch(operation.fetchProgress);
    operation.lifetime.abort(new OperationNotRetainedError(operation.id));
    operation.frame.releaseOwner();
    for (const lane of lanesOf(operation)) this.wakeLane(lane);
    this.wakeWaiting();
    this.changed.pulse();
  }

  /**
   * Drops the retained operations and ends their replays, because no other
   * session can replay their frames. A call keeps the lane it holds and starts
   * again under the next session, so the calls in a lane keep their order.
   */
  reset(): void {
    for (const operation of this.operations.values())
      if (operation.retained || operation.resolving) this.complete(operation);
  }

  private releaseFetch(fetch: FetchProgress): void {
    if (!this.fetchProgress.delete(fetch.id)) return;
    this.fetchBytes -= fetch.size;
    for (const lane of fetch.targets.keys())
      if (this.blockedProgress.get(lane) === fetch) this.blockedProgress.delete(lane);
  }

  private releaseSlot(operation: ClientOperation): void {
    if (!operation.holdsSlot)
      return;
    operation.holdsSlot = false;
    this.count -= 1;
  }

  private join(lane: string, waiting: AdmissionWaiter): LaneWaiters {
    let queue = this.waiting.get(lane);
    if (!queue) {
      queue = {};
      this.waiting.set(lane, queue);
    }
    waiting.previous = queue.tail;
    if (queue.tail) queue.tail.next = waiting;
    else queue.head = waiting;
    queue.tail = waiting;
    return queue;
  }

  private leave(lane: string, queue: LaneWaiters, waiting: AdmissionWaiter): void {
    if (waiting.previous) waiting.previous.next = waiting.next;
    else queue.head = waiting.next;
    if (waiting.next) waiting.next.previous = waiting.previous;
    else queue.tail = waiting.previous;
    this.capacityWaiting.delete(waiting);
    if (!queue.head) this.waiting.delete(lane);
    this.wakeLane(lane);
    this.wakeWaiting();
  }

  private busy(lane: string): boolean {
    return this.lanes.has(lane) || this.held.has(lane);
  }

  private wakeLane(lane: string): void {
    const head = this.waiting.get(lane)?.head;
    if (head && !this.busy(lane)) this.capacityWaiting.add(head);
  }

  private wakeWaiting(): void {
    for (const waiter of this.capacityWaiting) {
      if ((waiter.retainable && this.count >= this.maxOperations) ||
          waiter.size > this.maxBytes - this.bytes) continue;
      waiter.changed.pulse();
      return;
    }
  }

  async acquireResolution(operation: ClientOperation, deadline: number, signal: AbortSignal): Promise<() => void> {
    try {
      while (operation.resolving) {
        await waitWithin(this.changed.promise, deadline, signal);
      }
    } catch (error) {
      throw error instanceof VsrResponseTimeoutError ? new OperationUncertainError(operation.id, error) : error;
    }
    if (!this.owns(operation))
      throw new OperationNotRetainedError(operation.id);
    operation.resolving = true;
    return () => {
      operation.resolving = false;
      // An abandon left the lane to this replay until now.
      if (!operation.retained) this.complete(operation);
      this.changed.pulse();
    };
  }
}
