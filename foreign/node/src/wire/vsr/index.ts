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

import { createRequire } from 'node:module';
import type { CommandResponse } from '../../client/client.type.js';
import { COMMAND_CODE } from '../command.code.js';
import { responseError } from '../error.utils.js';
import { HEADER_SIZE, encodeRequestHeader } from './header.js';
import { Operation, operationForCode } from './operation.js';
import {
  deserializeLoginRegister,
  serializeLoginRegister,
  serializeLoginRegisterWithPat,
} from './register.js';
import { decodeResponse } from './reply.js';
import { ConsensusSession } from './session.js';

const packageMetadata = createRequire(import.meta.url)(
  '../../../package.json'
) as { version: string };
const SDK_VERSION = packageMetadata.version;
const MAX_U32 = 0xFFFF_FFFF;
/** `IggyError::Unauthenticated`, matching the Rust SDK's unbound-session error. */
const UNAUTHENTICATED = 40;

export class VsrSession {
  private state: ConsensusSession;

  constructor(clientId?: bigint) {
    this.state = new ConsensusSession(clientId);
  }

  reset(): void {
    this.state = new ConsensusSession();
  }

  bind(session: bigint): void {
    this.state.bind(session);
  }

  get hasActivity(): boolean {
    return this.state.hasActivity;
  }

  encode(command: number, payload: Buffer): Buffer {
    const operation = registerCommand(command)
      ? Operation.Register
      : operationForCode(command);
    const size = HEADER_SIZE + payload.length;
    if (size > MAX_U32)
      throw new RangeError('VSR request exceeds the u32 frame-size limit');

    let request: bigint;
    let session: bigint;

    if (operation === Operation.Register) {
      request = this.state.beginRegister();
      session = 0n;
    } else if (operation === Operation.NonReplicated) {
      request = this.state.currentRequestId();
      session = this.state.session ?? 0n;
    } else {
      if (this.state.session === null)
        throw responseError(command, UNAUTHENTICATED);
      // Partition ops consume an id too, even though no partition-plane dedup
      // exists yet: dedup needs each send to carry a distinct number, and the
      // metadata watermark tolerates the gaps.
      request = this.state.nextRequestId();
      session = this.state.session;
    }

    const header = encodeRequestHeader({
      size,
      client: this.state.clientId,
      request,
      operation,
      session,
      nonReplicatedCode:
        operation === Operation.NonReplicated ? command : undefined,
    });
    return payload.length === 0 ? header : Buffer.concat([header, payload]);
  }
}

export const decodeVsrResponse = (
  frame: Buffer,
  commandCode = 0
): CommandResponse => {
  const data = decodeResponse(frame, commandCode);
  return { status: 0, length: data.length, data };
};

export const readRegisteredSession = (response: CommandResponse): bigint =>
  deserializeLoginRegister(response.data).session;

export const prepareVsrCommand = (
  command: number,
  payload: Buffer
): { command: number, payload: Buffer } => {
  if (command === COMMAND_CODE.LoginUser) {
    const username = readWireName(payload, 0);
    const password = readWireName(payload, username.next);
    return {
      command: COMMAND_CODE.LoginRegister,
      payload: serializeLoginRegister(username.value, password.value, SDK_VERSION),
    };
  }
  if (command === COMMAND_CODE.LoginWithAccessToken) {
    const token = readWireName(payload, 0);
    return {
      command: COMMAND_CODE.LoginRegisterWithAccessToken,
      payload: serializeLoginRegisterWithPat(token.value, SDK_VERSION),
    };
  }
  return { command, payload };
};

const registerCommand = (command: number): boolean =>
  command === COMMAND_CODE.LoginRegister ||
  command === COMMAND_CODE.LoginRegisterWithAccessToken;

const readWireName = (
  payload: Buffer,
  offset: number
): { value: string, next: number } => {
  if (payload.length <= offset)
    throw new Error('wire name length is missing');
  const length = payload.readUInt8(offset);
  const next = offset + 1 + length;
  if (length === 0 || payload.length < next)
    throw new Error('wire name is incomplete');
  return { value: payload.subarray(offset + 1, next).toString('utf8'), next };
};

export { HEADER_SIZE } from './header.js';
export { VsrEvictionError } from './reply.js';
