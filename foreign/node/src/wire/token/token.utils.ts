
import { toDate } from '../serialize.utils.js';

export type CreateTokenResponse = {
  token: string
};

type TokenDeserialized = {
  bytesRead: number,
  data: CreateTokenResponse
};

export type Token = {
  name: string,
  expiry: Date | null
}

type TokenSerialized = {
  bytesRead: number,
  data: Token
}

export const deserializeCreateToken = (p: Buffer, pos = 0): TokenDeserialized => {
  const len = p.readUInt8(pos);
  const token = p.subarray(pos + 1, pos + 1 + len).toString();
  return { bytesRead: 1 + len, data: { token } };
}

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
