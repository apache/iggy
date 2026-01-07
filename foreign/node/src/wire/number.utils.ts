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
 * Converts a boolean value to a 1-byte Buffer.
 *
 * @param v - Boolean value to convert
 * @returns Buffer containing 0x00 (false) or 0x01 (true)
 */
export const boolToBuf = (v: boolean) => {
  const b = Buffer.allocUnsafe(1);
  b.writeUInt8(!v ? 0 : 1);
  return b;
}

/**
 * Converts an 8-bit signed integer to a 1-byte Buffer.
 *
 * @param v - Signed integer value (-128 to 127)
 * @returns Buffer containing the value
 */
export const int8ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(1);
  b.writeInt8(v);
  return b;
}

/**
 * Converts a 16-bit signed integer to a 2-byte Buffer in little-endian format.
 *
 * @param v - Signed integer value (-32,768 to 32,767)
 * @returns Buffer containing the value in little-endian byte order
 */
export const int16ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(2);
  b.writeInt16LE(v);
  return b;
}

/**
 * Converts a 32-bit signed integer to a 4-byte Buffer in little-endian format.
 *
 * @param v - Signed integer value (-2,147,483,648 to 2,147,483,647)
 * @returns Buffer containing the value in little-endian byte order
 */
export const int32ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeInt32LE(v);
  return b;
}

/**
 * Converts a 64-bit signed BigInt to an 8-byte Buffer in little-endian format.
 *
 * @param v - BigInt value
 * @returns Buffer containing the value in little-endian byte order
 */
export const int64ToBuf = (v: bigint) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigInt64LE(v);
  return b;
}

/**
 * Converts an 8-bit unsigned integer to a 1-byte Buffer.
 *
 * @param v - Unsigned integer value (0 to 255)
 * @returns Buffer containing the value
 */
export const uint8ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(1);
  b.writeUInt8(v);
  return b;
}

/**
 * Converts a 16-bit unsigned integer to a 2-byte Buffer in little-endian format.
 *
 * @param v - Unsigned integer value (0 to 65,535)
 * @returns Buffer containing the value in little-endian byte order
 */
export const uint16ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(2);
  b.writeUInt16LE(v);
  return b;
}

/**
 * Converts a 32-bit unsigned integer to a 4-byte Buffer in little-endian format.
 *
 * @param v - Unsigned integer value (0 to 4,294,967,295)
 * @returns Buffer containing the value in little-endian byte order
 */
export const uint32ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32LE(v);
  return b;
}

/**
 * Converts a 64-bit unsigned BigInt to an 8-byte Buffer in little-endian format.
 *
 * @param v - Unsigned BigInt value
 * @returns Buffer containing the value in little-endian byte order
 */
export const uint64ToBuf = (v: bigint) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigUInt64LE(v);
  return b;
}

/**
 * Converts a 32-bit floating-point number to a 4-byte Buffer in little-endian format.
 *
 * @param v - Float value (IEEE 754 single precision)
 * @returns Buffer containing the value in little-endian byte order
 */
export const floatToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeFloatLE(v);
  return b;
}

/**
 * Converts a 64-bit floating-point number to an 8-byte Buffer in little-endian format.
 *
 * @param v - Double value (IEEE 754 double precision)
 * @returns Buffer containing the value in little-endian byte order
 */
export const doubleToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(8);
  b.writeDoubleLE(v);
  return b;
}

/**
 * Converts a BigInt to a 128-bit unsigned integer Buffer in little-endian format.
 *
 * @param num - BigInt value to convert
 * @param width - Width in bytes (default: 16)
 * @returns Buffer containing the value in little-endian byte order
 */
export function u128ToBuf(num: bigint, width = 16): Buffer {
  const hex = num.toString(16);
  const b = Buffer.from(hex.padStart(width * 2, '0').slice(0, width * 2), 'hex');
  return b.reverse();
}

/**
 * Converts a 128-bit unsigned integer Buffer in little-endian format to a BigInt.
 *
 * @param b - Buffer containing the value in little-endian byte order
 * @returns BigInt representation of the value
 */
export function u128LEBufToBigint(b: Buffer): bigint {
  const hex = b.reverse().toString('hex');
  return hex.length === 0 ? BigInt(0) :  BigInt(`0x${hex}`);
}

