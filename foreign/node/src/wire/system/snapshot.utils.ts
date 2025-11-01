
import { type ValueOf, reverseRecord } from "../../type.utils.js";
import { uint8ToBuf } from '../number.utils.js';

export const SnapshotType = {
  FilesystemOverview: 1,
  ProcessList: 2,
  ResourceUsage: 3,
  Test: 4,
  ServerLogs: 5,
  ServerConfig: 6,
  All: 100
} as const;

export type SnapshotType = typeof SnapshotType;
export type SnapshotTypeId = keyof SnapshotType;
export type SnapshotTypeValue = ValueOf<SnapshotType>;
export const ReverseSnapshotType = reverseRecord(SnapshotType);

export const SnapshotCompression = {
  Stored: 1,
  Deflated: 2,
  Bzip2: 3,
  Zstd: 4,
  Lzma: 5,
  Xz: 6,
} as const;

export type SnapshotCompression = typeof SnapshotCompression;
export type SnapshotCompressionId = keyof SnapshotCompression;
export type SnapshotCompressionValue = ValueOf<SnapshotCompression>;
export const ReverseSnapshotCompression = reverseRecord(SnapshotCompression);

export type Snapshot = {
  types: SnapshotTypeValue[],
  compression: SnapshotCompressionValue
};

const isValidSnapshotCompression = (c: number): c is SnapshotCompressionValue =>
  c in ReverseSnapshotCompression

const errInvalidSnapshotCompression = (c: number) => {
  throw new Error(`Invalid snapshot compression v = ${c}`);
}

const isValidSnapshotType = (t: number): t is SnapshotTypeValue =>
  t in ReverseSnapshotType;

const errInvalidSnapshotType = (t: number) => {
  throw new Error(`Invalid snapshot type t = ${t}`);
};

export const serializeSnapshot = ({ types, compression }: Snapshot) => {
  if (!isValidSnapshotCompression(compression))
    return errInvalidSnapshotCompression(compression);

  types.every(t => isValidSnapshotType(t) || errInvalidSnapshotType(t));

  return Buffer.concat([
    uint8ToBuf(compression),
    uint8ToBuf(types.length),
    ...types.map(t => uint8ToBuf(t)),
  ]);
}

export const deserializeSnapshot = (b: Buffer, offset = 0): Snapshot => {
  const compression = b.readUInt8(offset);
  if (!isValidSnapshotCompression(compression))
    return errInvalidSnapshotCompression(compression);

  const typeCount = b.readUInt8(offset + 1);
  const types: SnapshotTypeValue[] = [];
  for (let i = 0; i < typeCount; i++) {
    const t = b.readUint8(offset + 2 + i);
    if (!isValidSnapshotType(t))
      errInvalidSnapshotType(t);
    else
      types.push(t);
  }
  return { types, compression };
};
