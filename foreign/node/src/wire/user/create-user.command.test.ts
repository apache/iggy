
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { CREATE_USER } from './create-user.command.js';

describe('CreateUser', () => {

  describe('serialize', () => {

    const u1 = {
      id: 1,
      username: 'test-user',
      password: 'test-pwd',
      status: 1, // Active,
      // perms: undefined // @TODO
    };

    it('serialize username, password, status, permissions into buffer', () => {
      assert.deepEqual(
        CREATE_USER.serialize(u1).length,
        1 + u1.username.length + 1 + u1.password.length + 1 + 1 + 4 + 1
      );
    });

    it('throw on username < 1', () => {
      const u2 = { ...u1, username: '' };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on username > 255 bytes', () => {
      const u2 = { ...u1, username: "YoLo".repeat(65) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on username > 255 bytes - utf8 version', () => {
      const u2 = { ...u1, username: "¥Ø£Ø".repeat(33) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on password < 1', () => {
      const u2 = { ...u1, password: '' };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on password > 255 bytes', () => {
      const u2 = { ...u1, password: "yolo".repeat(65) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

    it('throw on password > 255 bytes - utf8 version', () => {
      const u2 = { ...u1, password: "¥Ø£Ø".repeat(33) };
      assert.throws(
        () => CREATE_USER.serialize(u2)
      );
    });

  });
});

