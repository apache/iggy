
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CREATE_GROUP } from './create-group.command.js';

describe('CreateGroup', () => {

  describe('serialize', () => {

    const s1 = {
      streamId: 5,
      topicId: 2,
      groupId: 1,
      name: 'test-group'
    };
    
    it('serialize group id & name into buffer', () => {
      assert.deepEqual(
        CREATE_GROUP.serialize(s1).length,
        6 + 6 + 5 + s1.name.length
      );
    });

    it('throw on name < 1', () => {
      const s2 = {...s1, name: ''}; 
      assert.throws(
        () => CREATE_GROUP.serialize(s2)
      );
    });

    it("throw on name > 255 bytes", () => {
      const s2 = { ...s1, name: "YoLo".repeat(65)};
      assert.throws(
        () => CREATE_GROUP.serialize(s2)
      );
    });

    it("throw on name > 255 bytes - utf8 version", () => {
      const s2 = { ...s1, name: "¥Ø£Ø".repeat(33)};
      assert.throws(
        () => CREATE_GROUP.serialize(s2)
      );
    });

  });
});
