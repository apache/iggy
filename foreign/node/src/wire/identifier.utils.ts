
const NUMERIC = 1;
const STRING = 2;

type NUMERIC = typeof NUMERIC;
type STRING = typeof STRING;

// type NumericId = {
//   kind: NUMERIC,
//   length: 4,
//   value: number
// };

// type StringId = {
//   kind: STRING,
//   length: number,
//   value: string
// };

// export type Identifier = NumericId | StringId;

export type Id = number | string;

export const serializeIdentifier = (id: Id): Buffer => {
  if ('string' === typeof id) {
    return serializeStringId(id);
  }
  if ('number' === typeof id) {
    return serializeNumericId(id);
  }
  throw new Error(`Unsuported id type (${id} - ${typeof id})`);
};

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

const serializeNumericId = (id: number): Buffer => {
  const b = Buffer.alloc(1 + 1 + 4);
  b.writeUInt8(NUMERIC);
  b.writeUInt8(4, 1);
  b.writeUInt32LE(id, 2);
  return b;
};
