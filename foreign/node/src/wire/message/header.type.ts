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

import { type ValueOf, reverseRecord } from "../../type.utils.js";

/**
 * Enumeration of header value types supported in message headers.
 * Each type maps to a numeric identifier for wire protocol encoding.
 */
export const HeaderKind = {
  Raw: 1,
  String: 2,
  Bool: 3,
  Int8: 4,
  Int16: 5,
  Int32: 6,
  Int64: 7,
  Int128: 8,
  Uint8: 9,
  Uint16: 10,
  Uint32: 11,
  Uint64: 12,
  Uint128: 13,
  Float: 14,
  Double: 15,
} as const;

/** Type alias for the HeaderKind object */
export type HeaderKind = typeof HeaderKind;
/** String literal type of header kind names */
export type HeaderKindId = keyof HeaderKind;
/** Numeric values of header kinds */
export type HeaderKindValue = ValueOf<HeaderKind>;
/** Reverse mapping from numeric value to header kind name */
export const ReverseHeaderKind = reverseRecord(HeaderKind);

/** Returns expected byte size for a header kind, or -1 for variable-size kinds */
export const expectedSize = (kind: number): number => {
  switch (kind) {
    case HeaderKind.Bool:
    case HeaderKind.Int8:
    case HeaderKind.Uint8:
      return 1;
    case HeaderKind.Int16:
    case HeaderKind.Uint16:
      return 2;
    case HeaderKind.Int32:
    case HeaderKind.Uint32:
    case HeaderKind.Float:
      return 4;
    case HeaderKind.Int64:
    case HeaderKind.Uint64:
    case HeaderKind.Double:
      return 8;
    case HeaderKind.Int128:
    case HeaderKind.Uint128:
      return 16;
    default:
      return -1;
  }
};

/** Raw binary header value */
export type HeaderValueRaw = {
  kind: HeaderKind["Raw"];
  value: Buffer;
};

/** String header value */
export type HeaderValueString = {
  kind: HeaderKind["String"];
  value: string;
};

/** Boolean header value */
export type HeaderValueBool = {
  kind: HeaderKind["Bool"];
  value: boolean;
};

/** Signed 8-bit integer header value */
export type HeaderValueInt8 = {
  kind: HeaderKind["Int8"];
  value: number;
};

/** Signed 16-bit integer header value */
export type HeaderValueInt16 = {
  kind: HeaderKind["Int16"];
  value: number;
};

/** Signed 32-bit integer header value */
export type HeaderValueInt32 = {
  kind: HeaderKind["Int32"];
  value: number;
};

/** Signed 64-bit integer header value */
export type HeaderValueInt64 = {
  kind: HeaderKind["Int64"];
  value: bigint;
};

/** Signed 128-bit integer header value */
export type HeaderValueInt128 = {
  kind: HeaderKind["Int128"];
  value: Buffer; // | ArrayBuffer // ?
};

/** Unsigned 8-bit integer header value */
export type HeaderValueUint8 = {
  kind: HeaderKind["Uint8"];
  value: number;
};

/** Unsigned 16-bit integer header value */
export type HeaderValueUint16 = {
  kind: HeaderKind["Uint16"];
  value: number;
};

/** Unsigned 32-bit integer header value */
export type HeaderValueUint32 = {
  kind: HeaderKind["Uint32"];
  value: number;
};

/** Unsigned 64-bit integer header value */
export type HeaderValueUint64 = {
  kind: HeaderKind["Uint64"];
  value: bigint;
};

/** Unsigned 128-bit integer header value */
export type HeaderValueUint128 = {
  kind: HeaderKind["Uint128"];
  value: Buffer; // | ArrayBuffer // ?
};

/** 32-bit floating point header value */
export type HeaderValueFloat = {
  kind: HeaderKind["Float"];
  value: number;
};

/** 64-bit floating point (double) header value */
export type HeaderValueDouble = {
  kind: HeaderKind["Double"];
  value: number;
};

// export type HeaderValue =
//   HeaderValueRaw |
//   HeaderValueString |
//   HeaderValueBool |
//   HeaderValueInt8 |
//   HeaderValueInt16 |
//   HeaderValueInt32 |
//   HeaderValueInt64 |
//   HeaderValueInt128 |
//   HeaderValueUint8 |
//   HeaderValueUint16 |
//   HeaderValueUint32 |
//   HeaderValueUint64 |
//   HeaderValueUint128 |
//   HeaderValueFloat |
//   HeaderValueDouble;

// export type Headers = Record<string, HeaderValue>;

/** Header key with raw bytes */
export type HeaderKeyRaw = {
  kind: HeaderKind["Raw"];
  value: Buffer;
};

/** Header key with string value */
export type HeaderKeyString = {
  kind: HeaderKind["String"];
  value: string;
};

/** Header key with Int32 value */
export type HeaderKeyInt32 = {
  kind: HeaderKind["Int32"];
  value: number;
};

/** Union type of all possible header key types */
export type HeaderKey = HeaderKeyRaw | HeaderKeyString | HeaderKeyInt32;
