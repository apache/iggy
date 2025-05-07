
import { TlsClient } from './client/index.js';
import { login, logout, getStats, ping } from './wire/index.js';

try {
  // create socket
  const cli = TlsClient({ host: '127.0.0.1', port: 8090 });
  const s = () => Promise.resolve(cli);

  // PING
  const r2 = await ping(s)()
  console.log('RESPONSE PING', r2);

  // LOGIN
  const r = await login(s)({ username: 'iggy', password: 'iggy' });
  console.log('RESPONSE_login', r);

  // GET_STATS
  const r_stats = await getStats(s)();
  console.log('RESPONSE_stats', r_stats);

  // LOGOUT
  const rOut = await logout(s)();
  console.log('RESPONSE LOGOUT', rOut);

} catch (err) {
  console.error('FAILED!', err);
}

process.exit(0);
