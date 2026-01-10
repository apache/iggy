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


/**
 * Creates a pool factory for managing RawClient instances.
 *
 * @param config - Client configuration
 * @returns Pool factory with create and destroy methods
 */
const createPoolFactory = (config: ClientConfig) => ({
  create: async function () {
    return getRawClient(config);
  },
  destroy: async function (client: RawClient) {
    return client.destroy();
  }
});

/**
 * Creates a client provider that uses connection pooling.
 * Automatically acquires and releases clients from the pool.
 *
 * @param config - Client configuration including pool size options
 * @returns Client provider function with attached pool reference
 */
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


/**
 * Iggy client with connection pooling support.
 * Manages a pool of connections for efficient resource utilization.
 */
export class Client extends CommandAPI {
  /** Client configuration */
  _config: ClientConfig
  /** Connection pool instance */
  _pool: Pool<RawClient>

  /**
   * Creates a new pooled client.
   *
   * @param config - Client configuration
   */
  constructor(config: ClientConfig) {
    const pcp = poolClientProvider(config);
    super(pcp);
    this._config = config;
    this._pool = pcp._pool;
  };

  /**
   * Destroys the client and drains all connections from the pool.
   */
  async destroy() {
    debug('destroying client pool. pool size is', this._pool.size);
    await this._pool.drain();
    await this._pool.clear();
    debug('destroyed client pool. pool size is', this._pool.size);
  }
}

/**
 * Creates a client provider that reuses a single connection.
 *
 * @param config - Client configuration
 * @returns Client provider function that always returns the same client
 */
const singleClientProvider = (config: ClientConfig) => {
  const c = getRawClient(config);
  return async function singleClientProvider() {
    return c;
  }
}

/**
 * Iggy client that uses a single persistent connection.
 * Suitable for applications that don't need connection pooling.
 */
export class SingleClient extends CommandAPI {
  /** Client configuration */
  _config: ClientConfig

  /**
   * Creates a new single-connection client.
   *
   * @param config - Client configuration
   */
  constructor(config: ClientConfig) {
    super(singleClientProvider(config));
    this._config = config;
  }

  /**
   * Destroys the client connection.
   */
  async destroy() {
    const s = await this.clientProvider();
    s.destroy();
  }
};


/**
 * Simple Iggy client wrapper around an existing RawClient.
 * Useful when you already have a RawClient instance.
 */
export class SimpleClient extends CommandAPI {
  /**
   * Creates a new simple client from an existing RawClient.
   *
   * @param client - Existing RawClient instance
   */
  constructor(client: RawClient) {
    super(() => Promise.resolve(client));
  }

  /**
   * Destroys the underlying client connection.
   */
  async destroy() {
    const s = await this.clientProvider();
    s.destroy();
  }

};

/**
 * Creates a SimpleClient with the given configuration.
 * Convenience function for quickly creating a client.
 *
 * @param config - Client configuration
 * @returns SimpleClient instance
 */
export const getClient = async (config: ClientConfig) => {
  const cli = getRawClient(config);
  return new SimpleClient(cli);
};
