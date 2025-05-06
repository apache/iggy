
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';

describe('e2e -> system', async () => {

  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials: { username: 'iggy', password: 'iggy' }
  });
  
  it('e2e -> system::ping', async () => {
    assert.ok(await c.system.ping());
  });

  it('e2e -> system::login', async () => {
    assert.deepEqual(
      await c.session.login({ username: 'iggy', password: 'iggy' }),
      { userId: 1 }
    )
  });

  it('e2e -> system::getStat', async () => {
    assert.deepEqual(
      Object.keys(await c.system.getStats()),
      [
        'processId', 'cpuUsage', 'totalCpuUsage', 'memoryUsage', 'totalMemory',
        'availableMemory', 'runTime', 'startTime', 'readBytes', 'writtenBytes',
        'messagesSizeBytes', 'streamsCount', 'topicsCount', 'partitionsCount',
        'segmentsCount', 'messagesCount', 'clientsCount', 'consumersGroupsCount',
        'hostname', 'osName', 'osVersion', 'kernelVersion'
      ]
    );
  });

  it('e2e -> system::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
