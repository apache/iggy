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

/**
 * Reply decode funnel, ported from `decode_response` in
 * `core/sdk/src/vsr.rs`. Strict order: frame length, frame discriminant
 * (evictions surface as typed errors, never a read timeout), declared size,
 * pre-commit denial status before any body decode, then the committed
 * result section for result-framed operations.
 */

import { ResponseError, responseError } from '../error.utils.js';
import {
  Command, EvictionReason, HEADER_SIZE,
  peekCommand, readEviction, readReplyOperation, readSize, readStatus
} from './header.js';
import { Operation, isKnownOperation, isResultFramed } from './operation.js';

/** `IggyError` codes this funnel raises client-side. */
const INVALID_COMMAND = 3;
const UNAUTHENTICATED = 40;
const INVALID_CREDENTIALS = 42;
const INVALID_PERSONAL_ACCESS_TOKEN = 53;
const STALE_CLIENT = 30;
const INVALID_FORMAT = 4;
const EMPTY_RESPONSE = 304;
const INCOMPATIBLE_PROTOCOL_VERSION = 14003;

const RESULT_COUNT_LEN = 4;
const RESULT_ENTRY_LEN = 8;

export class VsrEvictionError extends ResponseError {
  constructor(errorCode: number) {
    super(0, errorCode);
    this.name = 'VsrEvictionError';
    Object.setPrototypeOf(this, VsrEvictionError.prototype);
  }
}

/**
 * Decodes one complete consensus frame into the reply body, stripping the
 * committed result section for result-framed operations and mapping every
 * denial channel to a thrown error.
 */
export const decodeResponse = (
  frame: Buffer,
  commandCode = 0
): Buffer => {
  if (frame.length < HEADER_SIZE)
    throw responseError(commandCode, EMPTY_RESPONSE);

  switch (peekCommand(frame)) {
    case Command.Eviction:
      throw evictionError(frame);
    case Command.Reply:
      break;
    default:
      throw responseError(commandCode, INVALID_COMMAND);
  }

  const size = readSize(frame);
  if (size < HEADER_SIZE || frame.length < size)
    throw responseError(commandCode, INVALID_COMMAND);

  const status = readStatus(frame);
  // A pre-commit denial always ships an empty body; the status channel and
  // the committed result section are mutually exclusive.
  if (status !== 0)
    throw responseError(commandCode, status);

  const operation = readReplyOperation(frame);
  if (!isKnownOperation(operation))
    throw responseError(commandCode, INVALID_COMMAND);
  const body = frame.subarray(HEADER_SIZE, size);
  return splitMetadataResult(operation, body, commandCode);
};

/**
 * Strips the committed result section leading a result-framed reply body.
 * A Register reply is result-framed too, except a terminal register failure
 * ships an empty body, passed through so the typed decode fails. A metadata
 * body too short for the entries its count claims is corruption, never a
 * silent success.
 */
export const splitMetadataResult = (
  operation: number,
  body: Buffer,
  commandCode = 0
): Buffer => {
  const resultFramed = isResultFramed(operation) ||
    (operation === Operation.Register && body.length > 0);
  if (!resultFramed) return body;

  if (body.length < RESULT_COUNT_LEN)
    throw responseError(commandCode, INVALID_COMMAND);
  const count = body.readUInt32LE(0);
  const sectionLength = RESULT_COUNT_LEN + count * RESULT_ENTRY_LEN;
  if (body.length < sectionLength)
    throw responseError(commandCode, INVALID_COMMAND);
  if (count > 0) {
    // First entry: {index u32, result u32}. The result shares the
    // IggyError space.
    const code = body.readUInt32LE(RESULT_COUNT_LEN + 4);
    if (code !== 0)
      throw responseError(commandCode, code);
  }
  return body.subarray(sectionLength);
};

/**
 * Maps a session-terminal eviction frame to a typed error, the same mapping
 * as `eviction_reason_to_error` in `core/common`. An invalid protocol window
 * degrades to an authentication error rather than trusting the remote frame.
 */
export const evictionError = (frame: Buffer): VsrEvictionError => {
  const eviction = readEviction(frame);
  let errorCode: number;
  switch (eviction.reason) {
    case EvictionReason.InvalidCredentials:
      errorCode = INVALID_CREDENTIALS;
      break;
    case EvictionReason.InvalidToken:
      errorCode = INVALID_PERSONAL_ACCESS_TOKEN;
      break;
    case EvictionReason.UserInactive:
    case EvictionReason.SessionError:
    case EvictionReason.NoSession:
    case EvictionReason.SessionTooLow:
    case EvictionReason.SessionReleaseMismatch:
      errorCode = UNAUTHENTICATED;
      break;
    case EvictionReason.StaleClient:
      errorCode = STALE_CLIENT;
      break;
    case EvictionReason.IncompatibleProtocol:
      if (eviction.serverProtocolVersionMin === 0 ||
        eviction.serverProtocolVersion < eviction.serverProtocolVersionMin)
        errorCode = UNAUTHENTICATED;
      else
        errorCode = INCOMPATIBLE_PROTOCOL_VERSION;
      break;
    case EvictionReason.MalformedLogin:
      errorCode = INVALID_FORMAT;
      break;
    case EvictionReason.Reserved:
    case EvictionReason.ClientReleaseTooLow:
    case EvictionReason.ClientReleaseTooHigh:
    case EvictionReason.InvalidRequestOperation:
    case EvictionReason.InvalidRequestBody:
    case EvictionReason.InvalidRequestBodySize:
      errorCode = INVALID_COMMAND;
      break;
    default:
      errorCode = UNAUTHENTICATED;
  }
  return new VsrEvictionError(errorCode);
};
