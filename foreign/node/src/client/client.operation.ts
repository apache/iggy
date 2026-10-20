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

const INVALID_MESSAGE_LENGTH = 80;
const TRANSIENT_NOT_ACCEPTED = 58;
export const OPERATION_TIMEOUT_MS = 30_000;

export class OperationUncertainError extends Error {
  constructor(readonly operationId: bigint | undefined, cause: unknown) {
    super('The operation outcome is unknown.', { cause });
    this.name = 'OperationUncertainError';
  }
}

export class LaneBlockedError extends Error {
  constructor(readonly operationId: bigint) {
    super('A retained operation blocks this mutation lane.');
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
  readonly command: number,
  readonly context: PartitionContext,
  readonly generation: number,
  frame: FrozenFrame,
  id: bigint,
  readonly retainable: boolean,
  readonly lifetime: AbortController,
  resolving: boolean,
  retained: boolean,
  uncertain: boolean,
  /** An abandon frees the slot of a running replay before the replay ends. */
  holdsSlot: boolean
};

type AdmissionWaiter = {
  size: number, retainable: boolean, changed: ChangeSignal,
  previous?: AdmissionWaiter, next?: AdmissionWaiter
};
type LaneWaiters = { head?: AdmissionWaiter, tail?: AdmissionWaiter };

export class MutationLanes {
  private readonly lanes = new Map<string, ClientOperation>();
  private readonly operations = new Map<bigint, ClientOperation>();
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

  async enter(lane: string, command: number, body: Buffer, context: PartitionContext,
    generation: number, retainable: boolean, session: VsrSession, deadline: number,
    signal: AbortSignal): Promise<ClientOperation> {
    const size = retainable ? HEADER_SIZE + body.length : 0;
    if (!Number.isSafeInteger(size) || size > this.maxBytes)
      throw responseError(command, INVALID_MESSAGE_LENGTH, 'request exceeds the mutation byte limit');
    const waiting: AdmissionWaiter = { size, retainable, changed: new ChangeSignal() };
    let queue = this.waiting.get(lane);
    if (!queue) {
      queue = {};
      this.waiting.set(lane, queue);
    }
    waiting.previous = queue.tail;
    if (queue.tail) queue.tail.next = waiting;
    else queue.head = waiting;
    queue.tail = waiting;
    try {
      for (;;) {
        signal.throwIfAborted();
        if (Date.now() >= deadline)
          throw responseError(command, TRANSIENT_NOT_ACCEPTED);
        this.throwIfRetained(lane);
        const head = !this.lanes.has(lane) && queue.head === waiting;
        if (head &&
            (!retainable || this.count < this.maxOperations) && size <= this.maxBytes - this.bytes) {
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
            lane, command, context, generation, retainable,
            id: bytes.readBigUInt64LE(REQUEST_OFFSET.request),
            frame: this.freeze(bytes, size),
            lifetime: new AbortController(), resolving: false, retained: false, uncertain: false,
            holdsSlot: retainable
          };
          this.lanes.set(lane, operation);
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
      if (waiting.previous) waiting.previous.next = waiting.next;
      else queue.head = waiting.next;
      if (waiting.next) waiting.next.previous = waiting.previous;
      else queue.tail = waiting.previous;
      this.capacityWaiting.delete(waiting);
      if (!queue.head) this.waiting.delete(lane);
      this.wakeLane(lane);
      this.wakeWaiting();
    }
  }

  async restart(operation: ClientOperation, body: Buffer, session: VsrSession,
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
    const bytes = session.encode(operation.command, body, operation.context);
    this.bytes += size;
    this.operations.delete(operation.id);
    operation.id = bytes.readBigUInt64LE(REQUEST_OFFSET.request);
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

  throwIfRetained(lane: string): void {
    const operation = this.lanes.get(lane);
    if (operation?.retained)
      throw new LaneBlockedError(operation.id);
  }

  retained(id: bigint): ClientOperation {
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
      for (let waiter = this.waiting.get(operation.lane)?.head; waiter; waiter = waiter.next)
        waiter.changed.pulse();
      this.changed.pulse();
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
    this.lanes.delete(operation.lane);
    this.releaseSlot(operation);
    operation.lifetime.abort(new OperationNotRetainedError(operation.id));
    operation.frame.releaseOwner();
    this.wakeLane(operation.lane);
    this.wakeWaiting();
    this.changed.pulse();
  }

  reset(): void {
    for (const operation of this.operations.values())
      this.complete(operation);
  }

  private releaseSlot(operation: ClientOperation): void {
    if (!operation.holdsSlot)
      return;
    operation.holdsSlot = false;
    this.count -= 1;
  }

  private wakeLane(lane: string): void {
    const head = this.waiting.get(lane)?.head;
    if (head && !this.lanes.has(lane)) this.capacityWaiting.add(head);
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
