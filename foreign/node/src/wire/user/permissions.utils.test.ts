
import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { serializePermissions, deserializePermissions } from './permissions.utils.js';

describe('Permissions', () => {

  const permissions = {
    global: {
      ManageServers: true,
      ReadServers: true,
      ManageUsers: true,
      ReadUsers: true,
      ManageStreams: true,
      ReadStreams: true,
      ManageTopics: true,
      ReadTopics: true,
      PollMessages: true,
      SendMessages: true
    },
    streams: []
  };

  it('serialize/deserialize', () => {
    const s = serializePermissions(permissions);
    const d = deserializePermissions(s);
    assert.deepEqual(permissions, d);
  });

});
