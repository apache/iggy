
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { serializeIdentifier } from './identifier.utils.js';

describe('serializeIdentifier', () => {

  it('serialize numeric id into buffer', () => {
    assert.deepEqual(
      serializeIdentifier(123).length,
      1 + 1 + 4
    );
  });

  it('serialize numeric string into buffer', () => {
    const strId = 'Groot';
    assert.deepEqual(
      serializeIdentifier(strId).length,
      1 + 1 + strId.length
    );
  });

  it('throw on empty string id', () => {
    assert.throws(
      () => serializeIdentifier(''),
    );
  });

  it('throw on string id > 255 bytes', () => {
    assert.throws(
      () => serializeIdentifier('YoLo'.repeat(65)),
    );
  });

  it('throw on string id > 255 bytes - utf8 version', () => {
    assert.throws(
      () => serializeIdentifier('¥Ø£Ø'.repeat(33)),
    );
  });

});
