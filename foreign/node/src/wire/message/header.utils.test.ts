
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { uuidv7, uuidv4 } from 'uuidv7'
import { serializeHeaders, deserializeHeaders, HeaderValue } from './header.utils.js';

describe('Headers', () => {

  const headers = {
    'p': HeaderValue.Bool(true),
    'x': HeaderValue.Uint32(123),
    'y': HeaderValue.Uint64(42n),
    'z': HeaderValue.Float(42.20000076293945),
    'a': HeaderValue.Double(1/3),
    'ID': HeaderValue.String(uuidv7()),
    'val': HeaderValue.Raw(Buffer.from(uuidv4()))
  };

  it('serialize/deserialize', () => {
    const s = serializeHeaders(headers);
    const d = deserializeHeaders(s);
    assert.deepEqual(headers, d);
  });

});
