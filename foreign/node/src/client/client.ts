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


import { createPool, type Pool } from 'generic-pool';
import type { RawClient, ClientConfig } from "./client.type.js"
import { getRawClient } from '../client/client.socket.js';
import { CommandAPI } from '../wire/command-set.js';
import { debug } from './client.debug.js';


// create & destroy must be async
const createPoolFactory = (config: ClientConfig) => ({
  create: async function () {
    return getRawClient(config);
  },
  destroy: async function (client: RawClient) {
    return client.destroy();
  }
});

const poolClientProvider = (config: ClientConfig) => {
  const min = config.poolSize?.min || 1;
  const max = config.poolSize?.max || 4;
  const pool = createPool(createPoolFactory(config), { min, max });
  const poolClientProvider = async () => {
    const c = await pool.acquire();
    debug('client acquired from pool. pool size is', pool.size);
    c.once('finishQueue', () => {
      pool.release(c)
      debug('client released to pool. pool size is', pool.size);
    });
    return c;
  }
  poolClientProvider._pool = pool;
  return poolClientProvider;
};


export class Client extends CommandAPI {
  _config: ClientConfig
  _pool: Pool<RawClient>

  constructor(config: ClientConfig) {
    const pcp = poolClientProvider(config);
    super(pcp);
    this._config = config;
    this._pool = pcp._pool;
  };

  async destroy() {
    debug('destroying client pool. pool size is', this._pool.size);
    await this._pool.drain();
    await this._pool.clear();
    debug('destroyed client pool. pool size is', this._pool.size);
  }
}

const singleClientProvider = (config: ClientConfig) => {
  const c = getRawClient(config);
  return async function singleClientProvider() {
    return c;
  }
}

export class SingleClient extends CommandAPI {
  _config: ClientConfig

  constructor(config: ClientConfig) {
    super(singleClientProvider(config));
    this._config = config;
  }

  async destroy() {
    const s = await this.clientProvider();
    s.destroy();
  }
};


export class SimpleClient extends CommandAPI {
  constructor(client: RawClient) {
    super(() => Promise.resolve(client));
  }

  async destroy() {
    const s = await this.clientProvider();
    s.destroy();
  }

};

export const getClient = async (config: ClientConfig) => {
  const cli = getRawClient(config);
  return new SimpleClient(cli);
};
