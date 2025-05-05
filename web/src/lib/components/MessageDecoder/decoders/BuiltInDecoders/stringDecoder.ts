import type { Decoder } from '../types';

export const stringDecoder: Decoder = {
  name: 'string',
  decode: (payload: string) => {
    return payload;
  }
};
