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

export type LoginResponse = {
  userId: number
}

export type LoginCredentials = {
  username: string,
  password: string,
  version?: string,
  context?: string
}

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
