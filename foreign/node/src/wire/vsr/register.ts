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

/**
 * The Register (login) handshake bodies and reply, ported from
 * `core/binary_protocol/src/requests/users/login_register.rs` and
 * `responses/users/login_register.rs`. The VSR server rejects the legacy login
 * codes with a `MalformedLogin` eviction, so a VSR client must emit
 * `LoginRegister` / `LoginRegisterWithPat` and nothing else.
 */

import { DeserializeError } from '../error.utils.js';

/**
 * Packed protocol semver of the wire contract this port implements,
 * `pack(0, 10, 3)` per `core/binary_protocol/src/version.rs`. Bump together
 * with the Rust `IGGY_PROTOCOL_VERSION` on any wire-incompatible change.
 */
export const IGGY_PROTOCOL_VERSION = (0 << 20) | (10 << 10) | 3;

const SDK_NAME = 'node-sdk';

const wireName = (value: string): Buffer => {
  const bytes = Buffer.from(value, 'utf8');
  if (bytes.length < 1 || bytes.length > 255)
    throw new Error(`wire name must be 1..255 bytes, got ${bytes.length}`);
  return Buffer.concat([Buffer.from([bytes.length]), bytes]);
};

const versionInfo = (sdkVersion: string): Buffer => {
  const version = Buffer.alloc(4);
  version.writeUInt32LE(IGGY_PROTOCOL_VERSION);
  return Buffer.concat([version, wireName(SDK_NAME), wireName(sdkVersion)]);
};

/**
 * `LoginRegisterRequest` body:
 * `[ClientVersionInfo][username len u8 + bytes][password len u8 + bytes]
 * [context len u32][context]`.
 */
export const serializeLoginRegister = (
  username: string,
  password: string,
  sdkVersion: string
): Buffer => {
  const passwordBytes = Buffer.from(password, 'utf8');
  if (passwordBytes.length > 255)
    throw new Error('password exceeds the u8 length prefix');
  return Buffer.concat([
    versionInfo(sdkVersion),
    wireName(username),
    Buffer.from([passwordBytes.length]),
    passwordBytes,
    Buffer.alloc(4)
  ]);
};

/** `LoginRegisterWithPatRequest` body: the PAT takes the credential slot. */
export const serializeLoginRegisterWithPat = (
  token: string,
  sdkVersion: string
): Buffer => {
  const tokenBytes = Buffer.from(token, 'utf8');
  if (tokenBytes.length > 255)
    throw new Error('token exceeds the u8 length prefix');
  return Buffer.concat([
    versionInfo(sdkVersion),
    Buffer.from([tokenBytes.length]),
    tokenBytes,
    Buffer.alloc(4)
  ]);
};

/** Decoded `LoginRegisterResponse`. */
export type LoginRegisterResponse = {
  userId: number,
  session: bigint,
  serverProtocolVersion: number,
  serverVersion: string
};

/**
 * `[user_id u32][session u64][server_protocol_version u32]
 * [server_version len u8 + bytes]`.
 */
export const deserializeLoginRegister = (
  body: Buffer
): LoginRegisterResponse => {
  if (body.length < 17)
    throw new DeserializeError(
      `login register response too short: ${body.length} bytes`);
  const versionLength = body.readUInt8(16);
  if (body.length < 17 + versionLength)
    throw new DeserializeError('login register response truncated');
  return {
    userId: body.readUInt32LE(0),
    session: body.readBigUInt64LE(4),
    serverProtocolVersion: body.readUInt32LE(12),
    serverVersion: body.subarray(17, 17 + versionLength).toString('utf8')
  };
};
