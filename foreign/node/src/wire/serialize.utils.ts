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


/**
 * Converts a microsecond timestamp (BigInt) to a JavaScript Date object.
 *
 * @param n - Timestamp in microseconds as BigInt
 * @returns JavaScript Date object
 */
export const toDate = (n: bigint): Date => new Date(Number(n / BigInt(1000)));

/**
 * Serializes a UUID string to a 16-byte Buffer.
 * Removes dashes from the UUID and converts to binary format.
 *
 * @param id - UUID string (e.g., "550e8400-e29b-41d4-a716-446655440000")
 * @returns 16-byte Buffer containing the UUID
 */
export const serializeUUID = (id: string) => Buffer.from(id.replaceAll('-', ''), 'hex');

/**
 * Deserializes a 16-byte Buffer to a UUID string.
 * Converts binary format to standard UUID string with dashes.
 *
 * @param p - 16-byte Buffer containing the UUID
 * @returns UUID string (e.g., "550e8400-e29b-41d4-a716-446655440000")
 */
export const deserializeUUID = (p: Buffer) => {
  const v = p.toString('hex');
  return `${v.slice(0, 8)}-` +
    `${v.slice(8, 12)}-${v.slice(12, 16)}-${v.slice(16, 20)}-` +
    `${v.slice(20, 32)}`;
};
