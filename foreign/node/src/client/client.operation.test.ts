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

import assert from 'node:assert/strict';
import { describe, it } from 'node:test';
import { COMMAND_CODE } from '../wire/command.code.js';
import { VsrSession } from '../wire/vsr/index.js';
import { HEADER_SIZE, REQUEST_OFFSET, EMPTY_PARTITION_CONTEXT } from '../wire/vsr/header.js';
import { MutationLanes, LaneBlockedError, OperationNotRetainedError } from './client.operation.js';
import { ResponseError } from '../wire/error.utils.js';

const body = Buffer.from('frozen body');
const frameSize = HEADER_SIZE + body.length;
const session = (): VsrSession => { const state = new VsrSession(1n); state.bind(42n); return state; };
const enter = (lanes: MutationLanes, state: VsrSession, lane: string,
  signal = new AbortController().signal, deadline = Date.now() + 1_000) =>
  lanes.enter(lane, COMMAND_CODE.CreateStream, body, EMPTY_PARTITION_CONTEXT, 1, true, state, deadline, signal);

describe('mutation admission and retained ownership', () => {
  it('retains one lane while independent groups keep their own order', async () => {
    const lanes = new MutationLanes(3, 3 * frameSize);
    const state = session();
    const first = await enter(lanes, state, 'first');
    first.uncertain = true;
    lanes.retain(first);
    await assert.rejects(enter(lanes, state, 'first'), (error: unknown) => error instanceof LaneBlockedError && error.operationId === first.id);
    const other = await enter(lanes, state, 'other');
    let admitted = false;
    const next = enter(lanes, state, 'other').then((operation) => { admitted = true; return operation; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(admitted, false);
    lanes.complete(other);
    lanes.complete(await next);
    assert.equal(lanes.retained(first.id), first);
    lanes.complete(first);
  });

  it('preserves FIFO when waiters at the head, middle and tail cancel', async () => {
    const count = 512;
    const lanes = new MutationLanes(1, frameSize);
    const state = session();
    const active = await enter(lanes, state, 'queued');
    const controllers = Array.from({ length: count }, () => new AbortController());
    const admitted: number[] = [];
    const pending = controllers.map((controller, index) =>
      enter(lanes, state, 'queued', controller.signal, Date.now() + 5_000).then((operation) => {
        admitted.push(index);
        lanes.complete(operation);
      }, (error: unknown) => { assert.equal(controller.signal.aborted, true, String(error)); }));
    const cancelled = new Set([0, Math.floor(count / 2), count - 1]);
    for (const index of cancelled) controllers[index].abort();
    lanes.complete(active);
    await Promise.all(pending);
    assert.deepEqual(admitted, controllers.map((_, index) => index).filter((index) => !cancelled.has(index)));
  });

  it('rejects a mutation that cannot fit a positive byte budget', async () => {
    const lanes = new MutationLanes(1, 1);
    await assert.rejects(enter(lanes, session(), 'metadata'), ResponseError);
  });

  for (const limit of ['count', 'bytes'] as const) {
    it(`keeps progress polls ordered without consuming the full mutation ${limit} budget`, async () => {
      const lanes = new MutationLanes(limit === 'count' ? 1 : 3, (limit === 'bytes' ? 1 : 3) * frameSize);
      const state = session();
      const cancel = new AbortController();
      const pending: Promise<unknown>[] = [];
      const enterProgress = (lane: string) => lanes.enter(lane, COMMAND_CODE.PollMessages,
        body, EMPTY_PARTITION_CONTEXT, 1, false, state, Date.now() + 1_000, cancel.signal);
      try {
        const retained = await enter(lanes, state, 'retained');
        retained.uncertain = true;
        lanes.retain(retained);
        await assert.rejects(enterProgress('retained'), (error: unknown) =>
          error instanceof LaneBlockedError && error.operationId === retained.id);
        const first = await enterProgress('progress');
        const originalId = first.id;
        await lanes.restart(first, body, state, Date.now() + 1_000, cancel.signal);
        assert.equal(first.id, originalId + 1n);
        let progressAdmitted = false;
        const progress = enterProgress('progress').then((operation) => { progressAdmitted = true; return operation; });
        pending.push(progress);
        let mutationAdmitted = false;
        const mutation = enter(lanes, state, 'other', cancel.signal).then((operation) => { mutationAdmitted = true; return operation; });
        pending.push(mutation);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(progressAdmitted, false);
        assert.equal(mutationAdmitted, false);
        lanes.complete(first);
        lanes.complete(await progress);
        await new Promise<void>((resolve) => setImmediate(resolve));
        assert.equal(mutationAdmitted, false);
        lanes.complete(retained);
        lanes.complete(await mutation);
      } finally {
        cancel.abort();
        lanes.reset();
        await Promise.allSettled(pending);
      }
    });
  }

  it('admits an independent lane when only a later same-lane waiter fits the free bytes', async () => {
    const sizes = { retained: 900, active: 300, large: 700, small: 500, independent: 500 };
    const lanes = new MutationLanes(2, 1_500);
    const state = session();
    const cancel = new AbortController();
    const pending: Promise<unknown>[] = [];
    const admitted: string[] = [];
    const enterFrame = (lane: string, size: number) => lanes.enter(lane, COMMAND_CODE.CreateStream,
      Buffer.alloc(size - HEADER_SIZE), EMPTY_PARTITION_CONTEXT, 1, true, state, Date.now() + 1_000, cancel.signal);
    try {
      const retained = await enterFrame('retained', sizes.retained);
      retained.uncertain = true;
      lanes.retain(retained);
      const active = await enterFrame('active', sizes.active);
      const large = enterFrame('queued', sizes.large).then((operation) => { admitted.push('large'); return operation; });
      const small = enterFrame('queued', sizes.small).then((operation) => { admitted.push('small'); return operation; });
      const independent = enterFrame('independent', sizes.independent).then((operation) => { admitted.push('independent'); return operation; });
      pending.push(large, small, independent);
      lanes.complete(active);
      await new Promise<void>((resolve) => setImmediate(resolve));
      assert.deepEqual(admitted, ['independent']);
      lanes.complete(await independent);
      lanes.complete(retained);
      lanes.complete(await large);
      lanes.complete(await small);
      assert.deepEqual(admitted, ['independent', 'large', 'small']);
    } finally {
      cancel.abort();
      lanes.reset();
      await Promise.allSettled(pending);
    }
  });

  it('releases logical count immediately but pins bytes through the final writer alias', async () => {
    const lanes = new MutationLanes(1, 2 * frameSize);
    const state = session();
    const first = await enter(lanes, state, 'first');
    first.frame.retain();
    lanes.complete(first);
    const second = await enter(lanes, state, 'second');
    assert.equal(second.id, first.id + 1n);
    second.frame.retain();
    lanes.complete(second);
    let admitted = false;
    const third = enter(lanes, state, 'third').then((operation) => { admitted = true; return operation; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(admitted, false);
    first.frame.release();
    lanes.complete(await third);
    second.frame.release();
  });

  it('checks oversize and expired admission before allocating an operation identity', async () => {
    const state = session();
    const lanes = new MutationLanes(1, HEADER_SIZE);
    await assert.rejects(enter(lanes, state, 'oversize'), (error: unknown) => error instanceof ResponseError && error.errorCode === 80);
    assert.equal(state.encode(COMMAND_CODE.Ping, Buffer.alloc(0)).readBigUInt64LE(REQUEST_OFFSET.request), 1n);
    const expired = new MutationLanes(1, frameSize);
    await assert.rejects(enter(expired, state, 'expired', undefined, Date.now() - 1),
      (error: unknown) => error instanceof ResponseError && error.errorCode === 58);
    assert.equal(state.encode(COMMAND_CODE.Ping, Buffer.alloc(0)).readBigUInt64LE(REQUEST_OFFSET.request), 2n);
  });

  it('reports a queued admission deadline as not accepted without consuming an identity', async () => {
    const state = session();
    const lanes = new MutationLanes(1, frameSize);
    const first = await enter(lanes, state, 'first');
    await assert.rejects(enter(lanes, state, 'second', undefined, Date.now() + 10),
      (error: unknown) => error instanceof ResponseError && error.errorCode === 58);
    lanes.complete(first);
    const next = await enter(lanes, state, 'second');
    assert.equal(next.id, first.id + 1n);
    lanes.complete(next);
  });

  it('keeps a restarting lane while its old writer pins the entire byte budget', async () => {
    const state = session();
    const lanes = new MutationLanes(1, frameSize);
    const first = await enter(lanes, state, 'first');
    const originalId = first.id;
    const writer = first.frame;
    writer.retain();
    let restarted = false;
    const restart = lanes.restart(first, body, state, Date.now() + 1_000, first.lifetime.signal)
      .then(() => { restarted = true; });
    let admitted = false;
    const later = enter(lanes, state, 'first').then((operation) => { admitted = true; return operation; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(restarted, false);
    assert.equal(admitted, false);
    writer.release();
    await restart;
    assert.equal(first.id, originalId + 1n);
    assert.equal(admitted, false);
    lanes.complete(first);
    const next = await later;
    assert.equal(next.id, first.id + 1n);
    lanes.complete(next);
    lanes.complete(await enter(lanes, state, 'independent'));
  });

  it('releases a reset restart without releasing an old writer twice', async () => {
    const state = session();
    const lanes = new MutationLanes(1, frameSize);
    const first = await enter(lanes, state, 'first');
    const writer = first.frame;
    writer.retain();
    const restart = lanes.restart(first, body, state, Date.now() + 1_000, first.lifetime.signal);
    lanes.reset();
    await assert.rejects(restart, OperationNotRetainedError);
    writer.release();
    lanes.complete(await enter(lanes, state, 'first'));
  });

  it('cancels budget waiters without copying bodies or consuming identities', async () => {
    const state = session();
    const lanes = new MutationLanes(1, frameSize);
    const first = await enter(lanes, state, 'first');
    const cancel = new AbortController();
    const waiting = enter(lanes, state, 'other', cancel.signal);
    cancel.abort();
    await assert.rejects(waiting);
    lanes.complete(first);
    const admitted = await enter(lanes, state, 'other');
    assert.equal(admitted.id, first.id + 1n);
    lanes.complete(admitted);
  });

  it('serializes concurrent resolvers and frees the slot but not the lane of an abandoned running replay', async () => {
    const lanes = new MutationLanes(1, 3 * frameSize);
    const state = session();
    const operation = await enter(lanes, state, 'first');
    operation.uncertain = true;
    lanes.retain(operation);
    const first = await lanes.acquireResolution(operation, Date.now() + 1_000, operation.lifetime.signal);
    const waiting = lanes.acquireResolution(operation, Date.now() + 1_000, operation.lifetime.signal);
    lanes.abandon(operation.id);
    assert.throws(() => lanes.retained(operation.id), OperationNotRetainedError);
    lanes.complete(await enter(lanes, state, 'other', undefined, Date.now() + 200));
    let admitted = false;
    const next = enter(lanes, state, 'first').then((later) => { admitted = true; return later; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(admitted, false);
    first();
    await assert.rejects(waiting, OperationNotRetainedError);
    lanes.complete(await next);
    const held = await enter(lanes, state, 'other');
    await assert.rejects(enter(lanes, state, 'last', undefined, Date.now() + 20), (error: unknown) =>
      error instanceof ResponseError && error.errorCode === 58);
    lanes.complete(held);
  });

  it('clears retained admission on reset without losing writer byte ownership', async () => {
    const state = session();
    const lanes = new MutationLanes(1, frameSize);
    const old = await enter(lanes, state, 'first');
    old.frame.retain();
    old.uncertain = true;
    lanes.retain(old);
    lanes.reset();
    assert.throws(() => lanes.retained(old.id), OperationNotRetainedError);
    state.reset();
    state.bind(43n);
    let admitted = false;
    const fresh = enter(lanes, state, 'fresh').then((operation) => { admitted = true; return operation; });
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(admitted, false);
    old.frame.release();
    const next = await fresh;
    assert.ok(next.id > old.id);
    lanes.complete(next);
  });
});
