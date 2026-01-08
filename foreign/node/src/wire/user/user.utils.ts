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


import { toDate } from '../serialize.utils.js';
import { deserializePermissions, type UserPermissions } from './permissions.utils.js';

/**
 * Basic user information without permissions.
 */
export type BaseUser = {
  /** User ID */
  id: number,
  /** User creation timestamp */
  createdAt: Date,
  /** User status (Active/Inactive) */
  status: string,
  /** Username */
  userName: string
};

/**
 * Result of deserializing a base user.
 */
type BaseUserDeserialized = {
  /** Number of bytes consumed */
  bytesRead: number,
  /** Deserialized user data */
  data: BaseUser
};

/** User with permissions information */
export type User = BaseUser & { permissions: UserPermissions | null };

/**
 * User status enumeration.
 */
export enum UserStatus {
  /** Active user */
  Active = 1,
  /** Inactive user */
  Inactive = 2,
};

/**
 * Converts a numeric status code to a status string.
 *
 * @param t - Numeric status code
 * @returns Status string ('Active', 'Inactive', or unknown)
 */
const statusString = (t: number): string => {
  switch (t.toString()) {
    case '1': return 'Active';
    case '2': return 'Inactive';
    default: return `unknown_status_${t}`;
    // default: throw new Error(`unknown_status_${t}`);
  }
}

/**
 * Deserializes a base user from a buffer.
 *
 * @param p - Buffer containing serialized user data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized user data
 * @throws Error if the buffer is empty (user does not exist)
 */
export const deserializeBaseUser = (p: Buffer, pos = 0): BaseUserDeserialized => {
  if (p.length === 0)
    throw new Error('User does not exist');

  const id = p.readUInt32LE(pos);
  const createdAt = toDate(p.readBigUInt64LE(pos + 4))
  const status = statusString(p.readUInt8(pos + 12));
  const userNameLength = p.readUInt8(pos + 13);
  const userName = p.subarray(pos + 14, pos + 14 + userNameLength).toString();

  return {
    bytesRead: 14 + userNameLength,
    data: {
      id,
      createdAt,
      status,
      userName,
    }
  }
};

/**
 * Deserializes a user with permissions from a buffer.
 *
 * @param p - Buffer containing serialized user data
 * @param pos - Starting position in the buffer
 * @returns Deserialized user with permissions
 */
export const deserializeUser = (p: Buffer, pos = 0): User => {
  const { bytesRead, data } = deserializeBaseUser(p, pos);
  pos += bytesRead;
  const hasPerm = 1 === p.readUInt8(pos);

  let permissions = null;
  if (hasPerm) {
    pos += 1;
    const permLength = p.readUInt32LE(pos);
    const permBuffer = p.subarray(pos + 4, pos + 4 + permLength);
    permissions = deserializePermissions(permBuffer, 0);
  }

  return { ...data, permissions };
};


/**
 * Deserializes multiple base users from a buffer.
 *
 * @param p - Buffer containing serialized users data
 * @param pos - Starting position in the buffer
 * @returns Array of deserialized base users
 */
export const deserializeUsers = (p: Buffer, pos = 0): BaseUser[] => {
  const users = [];
  const end = p.length;
  while (pos < end) {
    const { bytesRead, data } = deserializeBaseUser(p, pos);
    users.push(data);
    pos += bytesRead;
  }
  return users;
};
