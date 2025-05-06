
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LOGIN } from './login.command.js';

describe("Login Command", () => {

  // @warn use ascii char to keep char.length === byteLength

  const l1 = {
    username: 'iggyYolo',
    password: 'unitTestSeCret'
  };

  it("serialize credentials into a buffer", () => {
    assert.deepEqual(
      LOGIN.serialize(l1).length,
      2 + l1.username.length + l1.password.length + 4 + 4
    );
  });

  it("throw on empty login", () => {
    const l2 = {...l1, username: ''};
    assert.throws(
      () => LOGIN.serialize(l2)
    );
  });

  it("throw on empty password", () => {
    const l2 = {...l1, password: ''};
    assert.throws(
      () => LOGIN.serialize(l2)
    );
  });

  it("throw on login > 255 bytes", () => {
    const l2 = { ...l1, username: "YoLo".repeat(65)};
    assert.throws(
      () => LOGIN.serialize(l2)
    );
  });

  it("throw on login > 255 bytes - utf8 version", () => {
    const l2 = { ...l1, username: "¥Ø£Ø".repeat(33)};
    assert.throws(
      () => LOGIN.serialize(l2)
    );
  });

  it("throw on password > 255 bytes - utf8 version", () => {
    const l2 = { ...l1, password: "¥Ø£Ø".repeat(33)};
    assert.throws(
      () => LOGIN.serialize(l2)
    );
  });

});
