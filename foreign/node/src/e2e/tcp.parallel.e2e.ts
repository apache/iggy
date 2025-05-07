
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';

describe('e2e -> parallel', async () => {

  const credentials = { username: 'iggy', password: 'iggy' };
  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials
  });

  const baseGetMe = await c.client.getMe();


  it('e2e -> parallel::mix calls', async () => {
    const resp  = await Promise.all([
      c.client.getMe(),
      c.system.getStats(),
      c.system.ping(),
      c.session.login(credentials),
      c.client.getMe(),
      c.system.getStats(),
      c.system.ping(),
      c.session.login(credentials),
      c.client.getMe(),
      c.system.getStats(),
      c.system.ping(),
      c.session.login(credentials),
    ]);

    assert.ok(resp.length === 12);
    assert.deepEqual(resp[0], baseGetMe);
    assert.deepEqual(
      Object.keys(resp[1]),
      [
        'processId', 'cpuUsage', 'totalCpuUsage', 'memoryUsage', 'totalMemory',
        'availableMemory', 'runTime', 'startTime', 'readBytes', 'writtenBytes',
        'messagesSizeBytes', 'streamsCount', 'topicsCount', 'partitionsCount',
        'segmentsCount', 'messagesCount', 'clientsCount', 'consumersGroupsCount',
        'hostname', 'osName', 'osVersion', 'kernelVersion'
      ]
    );
    resp.forEach(r => assert.ok(r));
  });

  it('e2e -> parallel::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
