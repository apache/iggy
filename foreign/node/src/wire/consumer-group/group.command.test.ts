
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { DELETE_GROUP } from './delete-group.command.js';
import { JOIN_GROUP } from './join-group.command.js';
import { LEAVE_GROUP } from './leave-group.command.js';


const s1 = {
  streamId: 5,
  topicId: 2,
  groupId: 1,
};

describe('DeleteGroup', () => {
  describe('serialize', () => {
    
    it('serialize target group id into buffer', () => {
      assert.deepEqual(
        DELETE_GROUP.serialize(s1).length,
        6 + 6 + 6
      );
    });

  });
});

describe('JoinGroup', () => {
  describe('serialize', () => {
    
    it('serialize target group id into buffer', () => {
      assert.deepEqual(
        JOIN_GROUP.serialize(s1).length,
        6 + 6 + 6
      );
    });

  });
});

describe('LeaveGroup', () => {
  describe('serialize', () => {
    
    it('serialize target group id into buffer', () => {
      assert.deepEqual(
        LEAVE_GROUP.serialize(s1).length,
        6 + 6 + 6
      );
    });

  });
});
