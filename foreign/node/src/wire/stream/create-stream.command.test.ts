
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CREATE_STREAM } from './create-stream.command.js';

describe('CreateStream', () => {

  describe('serialize', () => {

    const s1 = {
      streamId: 1,
      name: 'test-stream'
    };
    
    it('serialize 1 numeric id & 1 name into buffer', () => {
      assert.deepEqual(
        CREATE_STREAM.serialize(s1).length,
        4 + 1 + s1.name.length
      );
    });

    it('throw on name < 1', () => {
      const s2 = {...s1, name: ''}; 
      assert.throws(
        () => CREATE_STREAM.serialize(s2)
      );
    });

    it("throw on name > 255 bytes", () => {
      const s2 = { ...s1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_STREAM.serialize(s2)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const s2 = { ...s1, name: "¥Ø£Ø".repeat(33)};
      assert.throws(
        () => CREATE_STREAM.serialize(s2)
      );
    });

  });
});
