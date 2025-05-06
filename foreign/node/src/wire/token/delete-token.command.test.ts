
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DELETE_TOKEN } from './delete-token.command.js';

describe('DeleteToken', () => {

  describe('serialize', () => {

    const t1 = {name : 'test-token'};

    it('serialize 1 name into buffer', () => {

      assert.deepEqual(
        DELETE_TOKEN.serialize(t1).length,
        1 + t1.name.length
      );
    });

    it('throw on name < 1', () => {
      const t2 = { ...t1, name: ''};
      assert.throws(
        () => DELETE_TOKEN.serialize(t2)
      );
    });

    it("throw on name > 255 bytes", () => {
      const t2 = { ...t1, name: "YoLo".repeat(65)};
      assert.throws(
        () => DELETE_TOKEN.serialize(t2)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const t2 = { ...t1, name: "¥Ø£Ø".repeat(33)};
      assert.throws(
        () => DELETE_TOKEN.serialize(t2)
      );
    });

  });
});
