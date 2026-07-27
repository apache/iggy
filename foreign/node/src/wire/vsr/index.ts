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
//

import type { CommandResponse } from '../../client/client.type.js';
import type { BinaryRequestKind } from '../command-set.js';
import { COMMAND_CODE } from '../command.code.js';
import { HEADER_SIZE, encodeRequestHeader } from './header.js';
import { namespaceForRequest } from './namespace.js';
import {
  Operation,
  isPartition,
  operationForCode,
} from './operation.js';
import {
  deserializeLoginRegister,
  serializeLoginRegister,
  serializeLoginRegisterWithPat,
} from './register.js';
import { decodeResponse } from './reply.js';
import { ConsensusSession } from './session.js';

const SDK_VERSION = '0.8.1-edge.3';

export class VsrSession {
  private state = new ConsensusSession();

  reset(): void {
    this.state = new ConsensusSession();
  }

  bind(session: bigint): void {
    this.state.bind(session);
  }

  encode(command: number, payload: Buffer, kind?: BinaryRequestKind): Buffer {
    const operation = registerCommand(command)
      ? Operation.Register
      : operationForCode(command, kind);
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
        throw new Error('VSR session is not registered');
      request = isPartition(operation)
        ? this.state.currentRequestId()
        : this.state.nextRequestId();
      session = this.state.session;
    }

    const header = encodeRequestHeader({
      size: HEADER_SIZE + payload.length,
      client: this.state.clientId,
      request,
      operation,
      namespace: namespaceForRequest(command, payload, operation),
      session,
      ...(operation === Operation.NonReplicated
        ? { nonReplicatedCode: command }
        : {}),
    });
    return payload.length === 0 ? header : Buffer.concat([header, payload]);
  }
}

export const decodeVsrResponse = (frame: Buffer): CommandResponse => {
  const data = decodeResponse(frame);
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
