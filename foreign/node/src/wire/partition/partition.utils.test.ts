
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { serializePartitionParams } from './partition.utils.js';

describe('serializePartitionParams', () => {

  it('serialize 2 numeric id & a uint32 into buffer', () => {
    assert.deepEqual(
      serializePartitionParams(12, 34, 512).length,
      6 + 6 + 4
    );
  });

  it('throw on partition count < 1', () => {
    assert.throws(
      () => serializePartitionParams(2, 1, 0),
    );
  });

  it('throw on partition count > 1000', () => {
    assert.throws(
      () => serializePartitionParams(1, 1, 1001),
    );
  });

});
