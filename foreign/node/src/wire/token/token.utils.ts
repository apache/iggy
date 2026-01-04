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

/**
 * Response from creating an access token.
 */
export type CreateTokenResponse = {
  /** The generated access token string */
  token: string
};

/**
 * Result of deserializing a token creation response.
 */
type TokenDeserialized = {
  /** Number of bytes consumed */
  bytesRead: number,
  /** Deserialized token response */
  data: CreateTokenResponse
};

/**
 * Access token information.
 */
export type Token = {
  /** Token name */
  name: string,
  /** Token expiry timestamp (null if no expiry) */
  expiry: Date | null
}

/**
 * Result of deserializing a token.
 */
type TokenSerialized = {
  /** Number of bytes consumed */
  bytesRead: number,
  /** Deserialized token data */
  data: Token
}

/**
 * Deserializes a token creation response from a buffer.
 *
 * @param p - Buffer containing serialized token response
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and token string
 */
export const deserializeCreateToken = (p: Buffer, pos = 0): TokenDeserialized => {
  const len = p.readUInt8(pos);
  const token = p.subarray(pos + 1, pos + 1 + len).toString();
  return { bytesRead: 1 + len, data: { token } };
}

/**
 * Deserializes a token from a buffer.
 *
 * @param p - Buffer containing serialized token data
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized token data
 */
export const deserializeToken = (p: Buffer, pos = 0): TokenSerialized => {
  const nameLength = p.readUInt8(pos);
  const name = p.subarray(pos + 1, pos + 1 + nameLength).toString();
  const rest = p.subarray(pos + 1 + nameLength);
  let expiry = null;
  let bytesRead = pos + 1 + nameLength;
  if (rest.length >= 8) {
    expiry = toDate(rest.readBigUInt64LE(0));
    bytesRead += 8;
  }
  return {
    bytesRead,
    data: {
      name,
      expiry
    }
  };
}

/**
 * Deserializes multiple tokens from a buffer.
 *
 * @param p - Buffer containing serialized tokens data
 * @param pos - Starting position in the buffer
 * @returns Array of deserialized tokens
 */
export const deserializeTokens = (p: Buffer, pos = 0): Token[] => {
  const tokens = [];
  const len = p.length;
  while (pos < len) {
    const { bytesRead, data } = deserializeToken(p, pos);
    tokens.push(data);
    pos += bytesRead;
  }
  return tokens;
};
