
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CREATE_TOKEN } from './create-token.command.js';

describe('CreateToken', () => {

  describe('serialize', () => {

    const t1 = {
      name: 'test-token',
      expiry: 1234n
    };

    it('serialize 1 name & 1 uint32 into buffer', () => {

      assert.deepEqual(
        CREATE_TOKEN.serialize(t1).length,
        8 + 1 + t1.name.length
      );
    });

    it('throw on name < 1', () => {
      const t2 = { ...t1, name: ''};
      assert.throws(
        () => CREATE_TOKEN.serialize(t2)
      );
    });

    it("throw on name > 255 bytes", () => {
      const t2 = { ...t1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_TOKEN.serialize(t2)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const t2 = { ...t1, name: "¥Ø£Ø".repeat(33)};
      assert.throws(
        () => CREATE_TOKEN.serialize(t2)
      );
    });

  });
});
