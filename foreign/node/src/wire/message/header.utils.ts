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

import {
  boolToBuf,
  int8ToBuf,
  int16ToBuf,
  int32ToBuf,
  int64ToBuf,
  uint8ToBuf,
  uint16ToBuf,
  uint32ToBuf,
  uint64ToBuf,
  floatToBuf,
  doubleToBuf,
} from "../number.utils.js";

import {
  type HeaderValueRaw,
  type HeaderValueString,
  type HeaderValueBool,
  type HeaderValueInt8,
  type HeaderValueInt16,
  type HeaderValueInt32,
  type HeaderValueInt64,
  type HeaderValueInt128,
  type HeaderValueUint8,
  type HeaderValueUint16,
  type HeaderValueUint32,
  type HeaderValueUint64,
  type HeaderValueUint128,
  type HeaderValueFloat,
  type HeaderValueDouble,
  type HeaderKindId,
  type HeaderKindValue,
  type HeaderKey,
  type HeaderKeyRaw,
  type HeaderKeyString,
  type HeaderKeyInt32,
  HeaderKind,
  ReverseHeaderKind,
  expectedSize,
} from "./header.type.js";

/**
 * Union type of all possible header value types.
 */
export type HeaderValue =
  | HeaderValueRaw
  | HeaderValueString
  | HeaderValueBool
  | HeaderValueInt8
  | HeaderValueInt16
  | HeaderValueInt32
  | HeaderValueInt64
  | HeaderValueInt128
  | HeaderValueUint8
  | HeaderValueUint16
  | HeaderValueUint32
  | HeaderValueUint64
  | HeaderValueUint128
  | HeaderValueFloat
  | HeaderValueDouble;

/** Header entry with key and value */
export type HeaderEntry = {
  key: HeaderKey;
  value: HeaderValue;
};

/** Array of header entries */
export type Headers = HeaderEntry[];

/**
 * Internal representation of a header value in binary format.
 */
type BinaryHeaderValue = {
  /** Header kind identifier */
  kind: number; // HeaderKind,
  /** Serialized value as Buffer */
  value: Buffer;
};

/**
 * Serializes a header value to a Buffer based on its kind.
 *
 * @param header - Header value to serialize
 * @returns Serialized value as Buffer
 */
export const serializeHeaderValue = (header: HeaderValue) => {
  const { kind, value } = header;
  switch (kind) {
    case HeaderKind.Raw:
      return value;
    case HeaderKind.String:
      return Buffer.from(value);
    case HeaderKind.Bool:
      return boolToBuf(value);
    case HeaderKind.Int8:
      return int8ToBuf(value);
    case HeaderKind.Int16:
      return int16ToBuf(value);
    case HeaderKind.Int32:
      return int32ToBuf(value);
    case HeaderKind.Int64:
      return int64ToBuf(value);
    case HeaderKind.Int128:
      return value;
    case HeaderKind.Uint8:
      return uint8ToBuf(value);
    case HeaderKind.Uint16:
      return uint16ToBuf(value);
    case HeaderKind.Uint32:
      return uint32ToBuf(value);
    case HeaderKind.Uint64:
      return uint64ToBuf(value);
    case HeaderKind.Uint128:
      return value;
    case HeaderKind.Float:
      return floatToBuf(value);
    case HeaderKind.Double:
      return doubleToBuf(value);
  }
};

/**
 * Serializes a header key to a Buffer based on its kind.
 *
 * @param key - Header key to serialize
 * @returns Serialized key as Buffer
 */
export const serializeHeaderKey = (key: HeaderKey): Buffer => {
  const { kind, value } = key;
  switch (kind) {
    case HeaderKind.Raw:
      return value;
    case HeaderKind.String:
      return Buffer.from(value);
    case HeaderKind.Int32:
      return int32ToBuf(value);
  }
};

/**
 * Serializes a single header key-value pair to wire format.
 * Format: [key_kind][key_length][key][value_kind][value_length][value]
 *
 * @param key - Header key
 * @param v - Binary header value
 * @returns Serialized header as Buffer
 */
export const serializeHeader = (key: HeaderKey, v: BinaryHeaderValue) => {
  const bKey = serializeHeaderKey(key);
  const keyHeader = Buffer.alloc(5);
  keyHeader.writeUInt8(key.kind);
  keyHeader.writeUInt32LE(bKey.length, 1);

  const valueHeader = Buffer.alloc(5);
  valueHeader.writeUInt8(v.kind);
  valueHeader.writeUInt32LE(v.value.length, 1);

  return Buffer.concat([keyHeader, bKey, valueHeader, v.value]);
};

/** Empty headers buffer constant */
export const EMPTY_HEADERS = Buffer.alloc(0);

/**
 * Creates a binary header value from a typed header value.
 *
 * @param header - Typed header value
 * @returns Binary header value
 */
const createHeaderValue = (header: HeaderValue): BinaryHeaderValue => ({
  kind: header.kind,
  value: serializeHeaderValue(header),
});

/**
 * Serializes all headers to a single buffer.
 *
 * @param headers - Optional headers array
 * @returns Serialized headers buffer (empty if no headers)
 */
export const serializeHeaders = (headers?: Headers) => {
  if (!headers || headers.length === 0) return EMPTY_HEADERS;

  return Buffer.concat(
    headers.map((entry) =>
      serializeHeader(entry.key, createHeaderValue(entry.value)),
    ),
  );
};

// deserialize ...

/** Possible JavaScript types for deserialized header values */
export type ParsedHeaderValue = boolean | string | number | bigint | Buffer;

/** Deserialized header key with kind and value */
export type ParsedHeaderKey = {
  kind: number;
  value: ParsedHeaderValue;
};

/** Deserialized header value with kind and value */
export type ParsedHeaderVal = {
  kind: number;
  value: ParsedHeaderValue;
};

/** Deserialized header entry */
export type ParsedHeaderEntry = {
  key: ParsedHeaderKey;
  value: ParsedHeaderVal;
};

/**
 * Result of deserializing a single header.
 */
type ParsedHeaderDeserialized = {
  /** Number of bytes consumed */
  bytesRead: number;
  /** Deserialized header entry */
  data: ParsedHeaderEntry;
};

/**
 * Maps a numeric header kind to its string identifier.
 *
 * @param k - Numeric header kind value
 * @returns Header kind identifier string
 * @throws Error if the header kind is unknown
 */
export const mapHeaderKind = (k: number): HeaderKindId => {
  if (!ReverseHeaderKind[k as HeaderKindValue])
    throw new Error(`unknown header kind: ${k}`);
  return ReverseHeaderKind[k as HeaderKindValue];
};

/**
 * Deserializes a header value buffer based on its kind.
 *
 * @param kind - Numeric header kind
 * @param value - Raw value buffer
 * @returns Deserialized value
 * @throws Error if the header kind is invalid
 */
export const deserializeHeaderValue = (
  kind: number,
  value: Buffer,
): ParsedHeaderValue => {
  switch (kind) {
    case HeaderKind.Int128:
    case HeaderKind.Uint128:
    case HeaderKind.Raw:
      return value;
    case HeaderKind.String:
      return value.toString();
    case HeaderKind.Int8:
      return value.readInt8();
    case HeaderKind.Int16:
      return value.readInt16LE();
    case HeaderKind.Int32:
      return value.readInt32LE();
    case HeaderKind.Int64:
      return value.readBigInt64LE();
    case HeaderKind.Uint8:
      return value.readUint8();
    case HeaderKind.Uint16:
      return value.readUint16LE();
    case HeaderKind.Uint32:
      return value.readUInt32LE();
    case HeaderKind.Uint64:
      return value.readBigUInt64LE();
    case HeaderKind.Bool:
      return value.readUInt8() === 1;
    case HeaderKind.Float:
      return value.readFloatLE();
    case HeaderKind.Double:
      return value.readDoubleLE();
    default:
      throw new Error(`deserializeHeaderValue: invalid HeaderKind ${kind}`);
  }
};

/**
 * Deserializes a single header from a buffer.
 * Format: [key_kind][key_length][key][value_kind][value_length][value]
 *
 * @param p - Buffer containing serialized headers
 * @param pos - Starting position in the buffer
 * @returns Object with bytes read and deserialized header data
 * @throws Error if header key or value length is invalid (must be 1-255)
 */
export const deserializeHeader = (
  p: Buffer,
  pos = 0,
): ParsedHeaderDeserialized => {
  const keyKind = p.readUInt8(pos);
  const keyLength = p.readUInt32LE(pos + 1);
  if (keyLength < 1 || keyLength > 255) {
    throw new Error(
      `Invalid header key length: ${keyLength}, must be between 1 and 255`,
    );
  }
  const keyExpected = expectedSize(keyKind);
  if (keyExpected !== -1 && keyLength !== keyExpected) {
    throw new Error(
      `Invalid header key size for kind ${keyKind}: expected ${keyExpected}, got ${keyLength}`,
    );
  }
  const keyValue = deserializeHeaderValue(
    keyKind,
    p.subarray(pos + 5, pos + 5 + keyLength),
  );
  pos += 5 + keyLength;

  const valueKind = p.readUInt8(pos);
  const valueLength = p.readUInt32LE(pos + 1);
  if (valueLength < 1 || valueLength > 255) {
    throw new Error(
      `Invalid header value length: ${valueLength}, must be between 1 and 255`,
    );
  }
  const valueExpected = expectedSize(valueKind);
  if (valueExpected !== -1 && valueLength !== valueExpected) {
    throw new Error(
      `Invalid header value size for kind ${valueKind}: expected ${valueExpected}, got ${valueLength}`,
    );
  }
  const value = deserializeHeaderValue(
    valueKind,
    p.subarray(pos + 5, pos + 5 + valueLength),
  );

  return {
    bytesRead: 5 + keyLength + 5 + valueLength,
    data: {
      key: { kind: keyKind, value: keyValue },
      value: { kind: valueKind, value },
    },
  };
};

/**
 * Deserializes all headers from a buffer.
 *
 * @param p - Buffer containing serialized headers
 * @param pos - Starting position in the buffer
 * @returns Array of parsed header entries
 */
export const deserializeHeaders = (p: Buffer, pos = 0): ParsedHeaderEntry[] => {
  const headers: ParsedHeaderEntry[] = [];
  const len = p.length;
  while (pos < len) {
    const { bytesRead, data } = deserializeHeader(p, pos);
    headers.push(data);
    pos += bytesRead;
  }
  return headers;
};

/**
 * HeaderValue factory functions and utilities.
 * Provides type-safe constructors for each header value type.
 */

/** Creates a raw binary header value */
const Raw = (value: Buffer): HeaderValueRaw => ({
  kind: HeaderKind.Raw,
  value,
});

/** Creates a string header value */
const String = (value: string): HeaderValueString => ({
  kind: HeaderKind.String,
  value,
});

/** Creates a boolean header value */
const Bool = (value: boolean): HeaderValueBool => ({
  kind: HeaderKind.Bool,
  value,
});

/** Creates an Int8 header value */
const Int8 = (value: number): HeaderValueInt8 => ({
  kind: HeaderKind.Int8,
  value,
});

/** Creates an Int16 header value */
const Int16 = (value: number): HeaderValueInt16 => ({
  kind: HeaderKind.Int16,
  value,
});

/** Creates an Int32 header value */
const Int32 = (value: number): HeaderValueInt32 => ({
  kind: HeaderKind.Int32,
  value,
});

/** Creates an Int64 header value */
const Int64 = (value: bigint): HeaderValueInt64 => ({
  kind: HeaderKind.Int64,
  value,
});

/** Creates an Int128 header value */
const Int128 = (value: Buffer): HeaderValueInt128 => ({
  kind: HeaderKind.Int128,
  value,
});

/** Creates a Uint8 header value */
const Uint8 = (value: number): HeaderValueUint8 => ({
  kind: HeaderKind.Uint8,
  value,
});

/** Creates a Uint16 header value */
const Uint16 = (value: number): HeaderValueUint16 => ({
  kind: HeaderKind.Uint16,
  value,
});

/** Creates a Uint32 header value */
const Uint32 = (value: number): HeaderValueUint32 => ({
  kind: HeaderKind.Uint32,
  value,
});

/** Creates a Uint64 header value */
const Uint64 = (value: bigint): HeaderValueUint64 => ({
  kind: HeaderKind.Uint64,
  value,
});

/** Creates a Uint128 header value */
const Uint128 = (value: Buffer): HeaderValueUint128 => ({
  kind: HeaderKind.Uint128,
  value,
});

/** Creates a Float header value */
const Float = (value: number): HeaderValueFloat => ({
  kind: HeaderKind.Float,
  value,
});

/** Creates a Double header value */
const Double = (value: number): HeaderValueDouble => ({
  kind: HeaderKind.Double,
  value,
});

/** Gets the kind identifier string of a header value */
const getKind = (h: HeaderValue) => mapHeaderKind(h.kind);
/** Gets the value from a header value */
const getValue = (h: HeaderValue) => h.value;

/**
 * HeaderValue factory object with constructors for all header types.
 */
export const HeaderValue = {
  Raw,
  String,
  Bool,
  Int8,
  Int16,
  Int32,
  Int64,
  Int128,
  Uint8,
  Uint16,
  Uint32,
  Uint64,
  Uint128,
  Float,
  Double,
  getKind,
  getValue,
};

/** Creates a raw binary header key */
const keyRaw = (value: Buffer): HeaderKeyRaw => ({
  kind: HeaderKind.Raw,
  value,
});

/** Creates a string header key */
const keyString = (value: string): HeaderKeyString => ({
  kind: HeaderKind.String,
  value,
});

/** Creates an Int32 header key */
const keyInt32 = (value: number): HeaderKeyInt32 => ({
  kind: HeaderKind.Int32,
  value,
});

export const HeaderKeyFactory = {
  Raw: keyRaw,
  String: keyString,
  Int32: keyInt32,
};

// export type InputHeaderValue = boolean | number | string | bigint | Buffer;
// export type InputHeaders = Record<string, InputHeaderValue>;

// const isFloat = (n: number) => n % 1 !== 0;

// export const createHeaderValueFloat = (v: number): HeaderValue =>
//   ({ kind: HeaderKind.Float, value: floatToBuf(v) });

// export const createHeaderValueDouble = (v: number): HeaderValue =>
//   ({ kind: HeaderKind.Double, value: doubleToBuf(v) });

// export const createHeaderValueInt32 = (v: number): HeaderValue =>
//   ({ kind: HeaderKind.Int32, value: int32ToBuf(v) });

// export const createHeaderValueInt64 = (v: bigint): HeaderValue =>
//   ({ kind: HeaderKind.Int64, value: int64ToBuf(v) });

// export const createHeaderValueUInt32 = (v: number): HeaderValue =>
//   ({ kind: HeaderKind.Uint32, value: uint32ToBuf(v) });

// export const createHeaderValueUInt64 = (v: bigint): HeaderValue =>
//   ({ kind: HeaderKind.Uint64, value: uint64ToBuf(v) });

// export const createHeaderValueBool = (v: boolean): HeaderValue =>
//   ({ kind: HeaderKind.Bool, value: boolToBuf(v) });

// export const createHeaderValueBuffer = (v: Buffer): HeaderValue =>
//   ({ kind: HeaderKind.Raw, value: v });

// export const createHeaderValueString = (v: string): HeaderValue =>
//   ({ kind: HeaderKind.String, value: Buffer.from(v) });

// // guess wire type from js type ?
// const guessHeaderValue = (v: InputHeaderValue): HeaderValue => {
//   if (typeof v === 'number') {
//     if (isFloat(v))
//       return createHeaderValueFloat(v);
//     else
//       return createHeaderValueInt32(v); // BAD KARMA
//   }
//   if (typeof v === 'bigint') {
//     return createHeaderValueInt64(v); // BAD KARMA
//   }
//   if (typeof v === 'boolean')
//     return createHeaderValueBool(v);
//   if (typeof v === 'string')
//     return createHeaderValueString(v);
//   if (v instanceof Buffer)
//     return createHeaderValueBuffer(v);

//   throw new Error(`unable to serialize headers param ${v} - ${typeof v}`)
// }
