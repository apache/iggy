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


/** Identifier kind for numeric IDs */
const NUMERIC = 1;
/** Identifier kind for string IDs */
const STRING = 2;

type NUMERIC = typeof NUMERIC;
type STRING = typeof STRING;

/**
 * Identifier type that can be either a numeric ID or a string name.
 * Used to identify streams, topics, partitions, and other resources.
 */
export type Id = number | string;

/**
 * Serializes an identifier (numeric or string) to a Buffer for wire protocol.
 *
 * @param id - Numeric ID or string name to serialize
 * @returns Buffer containing the serialized identifier
 * @throws Error if the identifier type is not supported
 */
export const serializeIdentifier = (id: Id): Buffer => {
  if ('string' === typeof id) {
    return serializeStringId(id);
  }
  if ('number' === typeof id) {
    return serializeNumericId(id);
  }
  throw new Error(`Unsupported id type (${id} - ${typeof id})`);
};

/**
 * Serializes a string identifier to a Buffer.
 *
 * @param id - String name to serialize (1-255 bytes)
 * @returns Buffer containing kind, length, and string bytes
 * @throws Error if the string length is not between 1 and 255 bytes
 */
const serializeStringId = (id: string): Buffer => {
  const b = Buffer.alloc(1 + 1);
  const bId = Buffer.from(id);
  if (bId.length < 1 || bId.length > 255)
    throw new Error('identifier/name should be between 1 and 255 bytes');
  b.writeUInt8(STRING);
  b.writeUInt8(bId.length, 1);
  return Buffer.concat([
    b,
    bId
  ]);
};

/**
 * Serializes a numeric identifier to a Buffer.
 *
 * @param id - Numeric ID to serialize (32-bit unsigned integer)
 * @returns Buffer containing kind, length, and ID bytes in little-endian format
 */
const serializeNumericId = (id: number): Buffer => {
  const b = Buffer.alloc(1 + 1 + 4);
  b.writeUInt8(NUMERIC);
  b.writeUInt8(4, 1);
  b.writeUInt32LE(id, 2);
  return b;
};
