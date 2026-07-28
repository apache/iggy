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
import { ConsensusSession } from './session.js';

describe('ConsensusSession', () => {
  it('starts unbound with a nonzero client ID', () => {
    const session = new ConsensusSession();
    assert.notEqual(session.clientId, 0n);
    assert.equal(session.session, null);
    assert.equal(session.currentRequestId(), 1n);
  });

  it('binds once to a positive server session', () => {
    const session = new ConsensusSession(7n);
    session.beginRegister();
    assert.throws(() => session.bind(0n), /must be > 0/);
    session.bind(42n);
    assert.equal(session.session, 42n);
    assert.throws(() => session.bind(43n), /already bound/);
  });

  it('advances metadata IDs monotonically', () => {
    const session = new ConsensusSession(7n);
    session.beginRegister();
    session.bind(42n);
    assert.equal(session.nextRequestId(), 1n);
    assert.equal(session.nextRequestId(), 2n);
    assert.equal(session.currentRequestId(), 3n);
  });

  it('rejects request IDs before binding and after exhaustion', () => {
    const session = new ConsensusSession(7n);
    assert.throws(
      () => session.nextRequestId(),
      /before bind/
    );
    session.bind(42n);
    (
      session as unknown as { requestCounter: bigint }
    ).requestCounter = 0xFFFF_FFFF_FFFF_FFFFn;
    assert.throws(
      () => session.nextRequestId(),
      /counter exhausted/
    );
  });

  it('rearms registration with a fresh client ID', () => {
    const session = new ConsensusSession(7n);
    session.beginRegister();
    session.bind(42n);
    session.beginRegister();
    assert.equal(session.session, null);
    assert.equal(session.currentRequestId(), 1n);
    assert.notEqual(session.clientId, 7n);
  });
});
