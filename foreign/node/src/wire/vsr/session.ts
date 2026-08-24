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

import { randomBytes } from 'node:crypto';

const MAX_U64 = 0xFFFF_FFFF_FFFF_FFFFn;

/**
 * Consensus-level session state, ported from `core/sdk/src/session.rs`.
 *
 * Each client instance generates an ephemeral random `clientId` (u128).
 * After a Register commits, the server assigns a `session` number (commit op
 * number). Replicated metadata requests advance a monotonic request watermark.
 * Non-replicated and partition-plane requests reuse the current value because
 * the server only applies request sequencing to replicated metadata.
 */
export class ConsensusSession {
  private _clientId: bigint;
  private _session: bigint | null;
  private requestCounter: bigint;
  private registerConsumed: boolean;

  constructor(clientId?: bigint) {
    this._clientId = clientId ?? generateClientId();
    this._session = null;
    this.requestCounter = 1n;
    this.registerConsumed = false;
  }

  get clientId(): bigint {
    return this._clientId;
  }

  /** The bound session number, or null before Register commits. */
  get session(): bigint | null {
    return this._session;
  }

  get isBound(): boolean {
    return this._session !== null;
  }

  get hasActivity(): boolean {
    return this.registerConsumed ||
      this._session !== null ||
      this.requestCounter > 1n;
  }

  /** Binds the session after Register commits through consensus. */
  bind(session: bigint): void {
    if (this._session !== null)
      throw new Error(`session already bound (session=${this._session})`);
    if (session <= 0n)
      throw new Error('session must be > 0');
    this._session = session;
  }

  /**
   * Begins a registration, re-arming the session for a re-login. A prior
   * consumed or bound session is replaced wholesale (fresh client id,
   * unbound), so a repeat login encodes a clean Register instead of tripping
   * the one-shot guard. Returns the register request id, always 0.
   */
  beginRegister(): bigint {
    if (this.registerConsumed || this.isBound) {
      this._clientId = generateClientId();
      this._session = null;
      this.requestCounter = 1n;
      this.registerConsumed = false;
    }
    this.registerConsumed = true;
    return 0n;
  }

  /**
   * Next application request id, advancing the counter: 1, 2, 3, ...
   * Request 0 is reserved for Register.
   */
  nextRequestId(): bigint {
    if (!this.isBound)
      throw new Error('nextRequestId called before bind');
    if (this.requestCounter === MAX_U64)
      throw new RangeError('VSR request counter exhausted');
    const id = this.requestCounter;
    this.requestCounter += 1n;
    return id;
  }

  /** Current counter value without advancing it. */
  currentRequestId(): bigint {
    return this.requestCounter;
  }
}

/** Ephemeral random u128, non-zero (retry on the astronomically unlikely 0). */
const generateClientId = (): bigint => {
  for (;;) {
    const id = randomBytes(16).reduce(
      (acc, byte) => (acc << 8n) | BigInt(byte), 0n);
    if (id !== 0n) return id;
  }
};
