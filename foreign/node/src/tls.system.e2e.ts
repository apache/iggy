/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */


import { getRawClient } from './client/index.js';
import { login, logout, getStats, ping } from './wire/index.js';

try {
  // create socket
  const cli = getRawClient({
    transport: 'TCP' as const,
    options: {
      host: '127.0.0.1',
      port: 8090
    },
    credentials: { username: 'iggy', password: 'iggy' }
  });
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
