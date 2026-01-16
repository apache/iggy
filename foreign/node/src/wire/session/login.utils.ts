/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import { uint32ToBuf, uint8ToBuf } from "../number.utils.js";

/**
 * Response from a successful login.
 */
export type LoginResponse = {
  /** The authenticated user's ID */
  userId: number
}

/**
 * Credentials for user login.
 */
export type LoginCredentials = {
  /** Username (1-255 bytes) */
  username: string,
  /** Password (1-255 bytes) */
  password: string,
  /** Optional client version string */
  version?: string,
  /** Optional client context string */
  context?: string
}

/**
 * Serializes login credentials for the login command.
 *
 * @param credentials - Login credentials object
 * @returns Buffer containing serialized login request
 * @throws Error if username or password is not between 1 and 255 bytes
 */
export const serializeLoginUser = ({
  username,
  password,
  version,
  context
}: LoginCredentials) => {
  const bUsername = Buffer.from(username);
  const bPassword = Buffer.from(password);

  if (bUsername.length < 1 || bUsername.length > 255)
    throw new Error('Username should be between 1 and 255 bytes');
  if (bPassword.length < 1 || bPassword.length > 255)
    throw new Error('Password should be between 1 and 255 bytes');

  const bUsernameLen = uint8ToBuf(bUsername.length);
  const bPasswordLen = uint8ToBuf(bPassword.length);
  const bVersion = Buffer.from(version || '');
  const bVersionLen = uint32ToBuf(bVersion.length)
  const bContext = Buffer.from(context || '');
  const bContextLen = uint32ToBuf(bContext.length)

  return Buffer.concat([
    bUsernameLen,
    bUsername,
    bPasswordLen,
    bPassword,
    bVersionLen,
    bVersion,
    bContextLen,
    bContext
  ]);
};


/**
 * Serializes a token for the login with token command.
 *
 * @param token - Access token string (1-255 bytes)
 * @returns Buffer containing serialized token login request
 * @throws Error if token is not between 1 and 255 bytes
 */
export const serializeLoginWithToken = (token: string) => {
  const bToken = Buffer.from(token);

  if (bToken.length < 1 || bToken.length > 255)
    throw new Error('Token length should be between 1 and 255 bytes');

  const b = uint8ToBuf(bToken.length);

  return Buffer.concat([
    b,
    bToken
  ]);
};
