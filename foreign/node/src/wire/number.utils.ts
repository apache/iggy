
export const boolToBuf = (v: boolean) => {
  const b = Buffer.allocUnsafe(1);
  b.writeUInt8(!v ? 0 : 1);
  return b;
}

export const int8ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(1);
  b.writeInt8(v);
  return b;
}

export const int16ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(2);
  b.writeInt16LE(v);
  return b;
}

export const int32ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeInt32LE(v);
  return b;
}

export const int64ToBuf = (v: bigint) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigInt64LE(v);
  return b;
}

export const uint8ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(1);
  b.writeUInt8(v);
  return b;
}

export const uint16ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(2);
  b.writeUInt16LE(v);
  return b;
}

export const uint32ToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeUInt32LE(v);
  return b;
}

export const uint64ToBuf = (v: bigint) => {
  const b = Buffer.allocUnsafe(8);
  b.writeBigUInt64LE(v);
  return b;
}

export const floatToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(4);
  b.writeFloatLE(v);
  return b;
}

export const doubleToBuf = (v: number) => {
  const b = Buffer.allocUnsafe(8);
  b.writeDoubleLE(v);
  return b;
}


// no js support ... use buffer or dataview or arraybuffer ?
// const uint128ToBuf = (v: Buffer) => { }
