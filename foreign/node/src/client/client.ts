// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { createPool, type Pool } from 'generic-pool';
import type {
  RawClient,
  ClientConfig,
  ClientConfigOrString,
  ClientEventMap
} from "./client.type.js"
import {
  CommandResponseStream,
  getRawClient
} from '../client/client.socket.js';
import { CommandAPI } from '../wire/command-set.js';
import { debug } from './client.debug.js';
import { normalizeClientConfig } from './client.config.js';


/**
 * Creates a client provider that uses connection pooling.
 * Automatically acquires and releases clients from the pool.
 *
 * @param config - Client configuration including pool size options
 * @returns Client provider, its connection pool and the pooled connection
 */
const createPooledClientProvider = (config: ClientConfig) => {
  // Constructed eagerly rather than inside the factory: generic-pool never
  // rejects waiting acquires when a create fails, and keeps re-dispatching
  // the doomed factory, which hangs every command. Normalization already
  // pins this pool to exactly one pooled connection, so there is nothing
  // lazy to preserve. Built directly rather than through getRawClient, whose
  // RawClient type has no way to remove a listener.
  const client = new CommandResponseStream(config);
  const minPoolSize = config.poolSize?.min || 1;
  const maxPoolSize = config.poolSize?.max || 4;
  const pool = createPool({
    create: async function () {
      return client;
    },
    destroy: async function () {
      return client.destroy();
    }
  }, {
    min: minPoolSize,
    max: maxPoolSize
  });
  const clientProvider = async () => {
    const pooled = await pool.acquire();
    debug('client acquired from pool. pool size is', pool.size);
    pooled.once('finishQueue', () => {
      pool.release(pooled)
      debug('client released to pool. pool size is', pool.size);
    });
    return pooled;
  }
  return { clientProvider, pool, client };
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
  /** The pooled connection, which emits the lifecycle events */
  private readonly events: CommandResponseStream

  /**
   * Creates a new pooled client.
   *
  * @param config - Client configuration or connection string
  */
  constructor(config: ClientConfigOrString) {
    const normalizedConfig = normalizeClientConfig(config);
    const { clientProvider, pool, client } =
      createPooledClientProvider(normalizedConfig);
    super(clientProvider);
    this._config = normalizedConfig;
    this._pool = pool;
    this.events = client;
  }

  /**
   * Registers a listener for a connection lifecycle event.
   *
   * @param event - Event name
   * @param listener - Called with the event's arguments
   * @returns This client
   */
  on<E extends keyof ClientEventMap>(
    event: E,
    listener: (...args: ClientEventMap[E]) => void
  ): this {
    this.events.on(event, listener);
    return this;
  }

  /**
   * Registers a listener that runs on the next occurrence of an event only.
   *
   * @param event - Event name
   * @param listener - Called with the event's arguments
   * @returns This client
   */
  once<E extends keyof ClientEventMap>(
    event: E,
    listener: (...args: ClientEventMap[E]) => void
  ): this {
    this.events.once(event, listener);
    return this;
  }

  /**
   * Removes a listener registered with `on` or `once`.
   *
   * @param event - Event name
   * @param listener - The listener to remove
   * @returns This client
   */
  off<E extends keyof ClientEventMap>(
    event: E,
    listener: (...args: ClientEventMap[E]) => void
  ): this {
    this.events.off(event, listener);
    return this;
  }

  /**
   * Alias of `off`. At run time, `events.once()` and `events.on()` from
   * `node:events` remove their listeners through this name.
   *
   * @param event - Event name
   * @param listener - The listener to remove
   * @returns This client
   */
  removeListener<E extends keyof ClientEventMap>(
    event: E,
    listener: (...args: ClientEventMap[E]) => void
  ): this {
    return this.off(event, listener);
  }

  /**
   * Destroys the client and drains all connections from the pool.
   * Resolves after the 'end' event.
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
const createSingleClientProvider = (config: ClientConfig) => {
  const client = getRawClient(config);
  return async function clientProvider() {
    return client;
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
  * @param config - Client configuration or connection string
  */
  constructor(config: ClientConfigOrString) {
    const normalizedConfig = normalizeClientConfig(config);
    super(createSingleClientProvider(normalizedConfig));
    this._config = normalizedConfig;
  }

  /**
   * Destroys the client connection.
   */
  async destroy() {
    const client = await this.clientProvider();
    await client.destroy();
  }
}


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
    const client = await this.clientProvider();
    await client.destroy();
  }

}

/**
 * Creates a SimpleClient with the given configuration.
 * Convenience function for quickly creating a client.
 *
 * @param config - Client configuration or connection string
 * @returns SimpleClient instance
 */
export const getClient = async (config: ClientConfigOrString) => {
  const client = getRawClient(config);
  return new SimpleClient(client);
};
