
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { LOGIN_WITH_TOKEN } from './login-with-token.command.js';


describe("Login with token Command", () => {

  // @warn use ascii char to keep char.length === byteLength
  const t1 = { token: 'thisIsBigSecretToken123' };

  it("serialize token into a buffer", () => {
    assert.deepEqual(
      LOGIN_WITH_TOKEN.serialize(t1).length,
      1 + t1.token.length
    );
  });

  it("throw on empty token", () => {
    assert.throws(
      () => LOGIN_WITH_TOKEN.serialize({token: ""})
    );
  });

  it("throw on token > 255 bytes", () => {
    assert.throws(
      () => LOGIN_WITH_TOKEN.serialize({token: "YoLo".repeat(65)})
    );
  });

  it("throw on login > 255 bytes - utf8 version", () => {
    assert.throws(
      () => LOGIN_WITH_TOKEN.serialize({token: "¥Ø£Ø".repeat(33)})
    );
  });

});
