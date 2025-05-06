import type { Decoder } from '../types';
import { parseNestedJson } from '$lib/utils/parsers';

export const jsonDecoder: Decoder = {
  name: 'json',
  decode: (payload: string) => {
    try {
      const decoded = parseNestedJson(payload);
      return JSON.stringify(decoded, null, 2);
    } catch {
      return null;
    }
  }
};
