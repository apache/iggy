
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
  doubleToBuf
} from '../number.utils.js';

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
  HeaderKind,
  ReverseHeaderKind
} from './header.type.js';


export type HeaderValue =
  HeaderValueRaw |
  HeaderValueString |
  HeaderValueBool |
  HeaderValueInt8 |
  HeaderValueInt16 |
  HeaderValueInt32 |
  HeaderValueInt64 |
  HeaderValueInt128 |
  HeaderValueUint8 |
  HeaderValueUint16 |
  HeaderValueUint32 |
  HeaderValueUint64 |
  HeaderValueUint128 |
  HeaderValueFloat |
  HeaderValueDouble;

export type Headers = Record<string, HeaderValue>;


type BinaryHeaderValue = {
  kind: number,// HeaderKind,
  value: Buffer
}

export const serializeHeaderValue = (header: HeaderValue) => {
  const { kind, value } = header;
  switch (kind) {
    case HeaderKind.Raw: return value;
    case HeaderKind.String: return Buffer.from(value);
    case HeaderKind.Bool: return boolToBuf(value);
    case HeaderKind.Int8: return int8ToBuf(value);
    case HeaderKind.Int16: return int16ToBuf(value);
    case HeaderKind.Int32: return int32ToBuf(value);
    case HeaderKind.Int64: return int64ToBuf(value);
    case HeaderKind.Int128: return value;
    case HeaderKind.Uint8: return uint8ToBuf(value);
    case HeaderKind.Uint16: return uint16ToBuf(value);
    case HeaderKind.Uint32: return uint32ToBuf(value);
    case HeaderKind.Uint64: return uint64ToBuf(value);
    case HeaderKind.Uint128: return value;
    case HeaderKind.Float: return floatToBuf(value);
    case HeaderKind.Double: return doubleToBuf(value);
  }
};


export const serializeHeader = (key: string, v: BinaryHeaderValue) => {
  const bKey = Buffer.from(key)
  const b1 = uint32ToBuf(bKey.length);
  const b2 = Buffer.alloc(5);
  b2.writeUInt8(v.kind);
  b2.writeUInt32LE(v.value.length, 1);

  // @TODO debug
  // console.log(
  //   'SERIALIZE\n',
  //   'KEY-LEN', b1.length, b1.toString('hex'), '=', b1.readUInt32LE(0), '\n',
  //   'KEY', bKey.length, bKey.toString('hex'), '\n',
  //   'KIND', b2.readUInt8(0), b2.subarray(0, 1).toString('hex'), '\n',
  //   'V-LEN', b2.readUInt32LE(1), b2.subarray(1, 5).toString('hex'), '\n',
  //   'V', v.value.length, v.value.toString('hex')
  // );

  return Buffer.concat([
    b1,
    bKey,
    b2,
    v.value
  ]);
};

export const EMPTY_HEADERS = Buffer.alloc(0);

const createHeaderValue = (header: HeaderValue): BinaryHeaderValue => ({
  kind: header.kind,
  value: serializeHeaderValue(header)
});

export const serializeHeaders = (headers?: Headers) => {
  if (!headers)
    return EMPTY_HEADERS;

  return Buffer.concat(Object.keys(headers).map(
    (c: string) => serializeHeader(c, createHeaderValue(headers[c]))
  ));
};

// deserialize ...

export type ParsedHeaderValue = boolean | string | number | bigint | Buffer;

export type ParsedHeader = {
  kind: ParsedHeaderValue,
  value: ParsedHeaderValue
}

type HeaderWithKey = ParsedHeader & { key: string };

export type HeadersMap = Record<string, ParsedHeader>;

type ParsedHeaderDeserialized = {
  bytesRead: number,
  data: HeaderWithKey
}

export const mapHeaderKind = (k: number): HeaderKindId => {
  if (!ReverseHeaderKind[k as HeaderKindValue])
    throw new Error(`unknow header kind: ${k}`);
  return ReverseHeaderKind[k as HeaderKindValue];
}

export const deserializeHeaderValue =
  (kind: number, value: Buffer): ParsedHeaderValue => {
    switch (kind) {
      case HeaderKind.Int128:
      case HeaderKind.Uint128:
      case HeaderKind.Raw: return value;
      case HeaderKind.String: return value.toString();
      case HeaderKind.Int8: return value.readInt8();
      case HeaderKind.Int16: return value.readInt16LE();
      case HeaderKind.Int32: return value.readInt32LE();
      case HeaderKind.Int64: return value.readBigInt64LE();
      case HeaderKind.Uint8: return value.readUint8();
      case HeaderKind.Uint16: return value.readUint16LE();
      case HeaderKind.Uint32: return value.readUInt32LE();
      case HeaderKind.Uint64: return value.readBigUInt64LE();
      case HeaderKind.Bool: return value.readUInt8() === 1;
      case HeaderKind.Float: return value.readFloatLE();
      case HeaderKind.Double: return value.readDoubleLE();
      default: throw new Error(`deserializeHeaderValue: invalid HeaderKind ${kind}`);
    }
  };

export const deserializeHeader = (p: Buffer, pos = 0): ParsedHeaderDeserialized => {
  const keyLength = p.readUInt32LE(pos);
  const key = p.subarray(pos + 4, pos + 4 + keyLength).toString();
  pos += keyLength + 4;
  const rawKind = p.readUInt8(pos);
  // @TODO ?
  // const kind = mapHeaderKind(rawKind); 
  const valueLength = p.readUInt32LE(pos + 1);
  const value = deserializeHeaderValue(rawKind, p.subarray(pos + 5, pos + 5 + valueLength));

  return {
    bytesRead: 4 + 4 + 1 + keyLength + valueLength,
    data: {
      key,
      kind: rawKind,
      value
    }
  };
}

export const deserializeHeaders = (p: Buffer, pos = 0) => {
  const headers: HeadersMap = {};
  const len = p.length;
  while (pos < len) {
    const { bytesRead, data: { kind, key, value } } = deserializeHeader(p, pos);
    headers[key] = { kind, value };
    pos += bytesRead;
  }
  return headers;
}

/** HeaderValue Helper */
const Raw = (value: Buffer): HeaderValueRaw => ({
  kind: HeaderKind.Raw,
  value
});

const String = (value: string): HeaderValueString => ({
  kind: HeaderKind.String,
  value
});

const Bool = (value: boolean): HeaderValueBool => ({
  kind: HeaderKind.Bool,
  value
});

const Int8 = (value: number): HeaderValueInt8 => ({
  kind: HeaderKind.Int8,
  value
});

const Int16 = (value: number): HeaderValueInt16 => ({
  kind: HeaderKind.Int16,
  value
});

const Int32 = (value: number): HeaderValueInt32 => ({
  kind: HeaderKind.Int32,
  value
});

const Int64 = (value: bigint): HeaderValueInt64 => ({
  kind: HeaderKind.Int64,
  value
});

const Int128 = (value: Buffer): HeaderValueInt128 => ({
  kind: HeaderKind.Int128,
  value
});

const Uint8 = (value: number): HeaderValueUint8 => ({
  kind: HeaderKind.Uint8,
  value
});

const Uint16 = (value: number): HeaderValueUint16 => ({
  kind: HeaderKind.Uint16,
  value
});

const Uint32 = (value: number): HeaderValueUint32 => ({
  kind: HeaderKind.Uint32,
  value
});

const Uint64 = (value: bigint): HeaderValueUint64 => ({
  kind: HeaderKind.Uint64,
  value
});

const Uint128 = (value: Buffer): HeaderValueUint128 => ({
  kind: HeaderKind.Uint128,
  value
});

const Float = (value: number): HeaderValueFloat => ({
  kind: HeaderKind.Float,
  value
});

const Double = (value: number): HeaderValueDouble => ({
  kind: HeaderKind.Double,
  value
});

const getKind = (h: HeaderValue) => mapHeaderKind(h.kind);
const getValue = (h: HeaderValue) => h.value;

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
  getValue
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
