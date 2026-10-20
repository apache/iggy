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

import type { RawClient, ClientConfig, ClientConfigOrString } from './client.type.js';
import { getRawClient } from './client.socket.js';
import { CommandAPI } from '../wire/command-set.js';
import { normalizeClientConfig } from './client.config.js';

/** Every facade call shares one session and its independently multiplexed replica links. */
export class Client extends CommandAPI {
  readonly _config: ClientConfig;

  constructor(config: ClientConfigOrString) {
    const normalized = normalizeClientConfig(config);
    const client = getRawClient(normalized);
    super(async () => client);
    this._config = normalized;
  }

  async destroy(): Promise<void> {
    (await this.clientProvider()).destroy();
  }
}

export class SingleClient extends Client {}

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
    client.destroy();
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
