import type { Decoder } from '../types';

export const xmlDecoder: Decoder = {
  name: 'xml',
  decode: (payload: string) => {
    try {
      const parser = new DOMParser();
      const doc = parser.parseFromString(payload, 'text/xml');
      if (doc.documentElement.nodeName === 'parsererror') {
        return null;
      }
      return payload;
    } catch {
      return null;
    }
  }
};
