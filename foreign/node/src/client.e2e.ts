
import assert from 'node:assert/strict';
import { Client } from './client/client.js';


try {

  const credentials = { username: 'iggy', password: 'iggy' };

  // pooled client
  const c = new Client({
    transport: 'TCP',
    options: { port: 8090, host: '127.0.0.1' },
    credentials
  });
  
  console.log('R1', await c.stream.list());
  console.log('R2' , await c.user.list());
  console.log('R3' , await c.client.list());
  console.log('R4' , await c.system.getStats());

  // serial call should keep socket pool to 1
  assert.equal(c._pool.size, 1);
  
  const resp  = await Promise.all([
    c.user.list(),
    c.stream.list(),
    c.system.getStats(),
    c.user.list(),
    c.stream.list(),
    c.system.getStats(),
    c.user.list(),
    c.stream.list(),
    c.system.getStats(),
  ]);

  console.log('parallel call resp', resp);

  // serial call should raise socket pool to MAX_POOL
  assert.equal(c._pool.size, 4);

} catch (err) {
  console.error('FAILED', err);
}

process.exit(0);
