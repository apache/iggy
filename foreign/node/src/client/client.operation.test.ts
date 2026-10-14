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
import { MutationLanes, LaneBlockedError, OperationNotRetainedError, VsrResponseTimeoutError } from './client.operation.js';
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
    await assert.rejects(enter(lanes, state, 'oversize'), (error: unknown) => error instanceof ResponseError && error.errorCode === 3);
    assert.equal(state.encode(COMMAND_CODE.Ping, Buffer.alloc(0)).readBigUInt64LE(REQUEST_OFFSET.request), 1n);
    const expired = new MutationLanes(1, frameSize);
    await assert.rejects(enter(expired, state, 'expired', undefined, Date.now() - 1), VsrResponseTimeoutError);
    assert.equal(state.encode(COMMAND_CODE.Ping, Buffer.alloc(0)).readBigUInt64LE(REQUEST_OFFSET.request), 2n);
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

  it('serializes concurrent resolvers and invalidates them on abandon', async () => {
    const lanes = new MutationLanes(1, frameSize);
    const operation = await enter(lanes, session(), 'first');
    operation.uncertain = true;
    lanes.retain(operation);
    const first = await lanes.acquireResolution(operation, Date.now() + 1_000, operation.lifetime.signal);
    const waiting = lanes.acquireResolution(operation, Date.now() + 1_000, operation.lifetime.signal);
    lanes.complete(operation);
    await assert.rejects(waiting, OperationNotRetainedError);
    first();
    assert.throws(() => lanes.retained(operation.id), OperationNotRetainedError);
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
