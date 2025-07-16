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


import { EventEmitter } from 'node:events';
import type {
  ClientConfig,
  ClientCredentials, CommandResponse,
  PasswordCredentials, RawClient, TokenCredentials
} from '../client/client.type.js';
import { handleResponse } from './client.utils.js';
import { responseError } from '../wire/error.utils.js';
import { debug } from './client.debug.js';
import { IggyConnection } from './client.connection.js';
import { LOGIN, LOGIN_WITH_TOKEN, PING } from '../wire/index.js';


const UNLOGGED_COMMAND_CODE = [
  PING.code,
  LOGIN.code,
  LOGIN_WITH_TOKEN.code
];

type Job = {
  command: number,
  payload: Buffer,
  resolve: (v: CommandResponse | PromiseLike<CommandResponse>) => void,
  reject: (e: unknown) => void
};


export class CommandResponseStream extends EventEmitter {
  private options: ClientConfig;
  private connection: IggyConnection;
  private _execQueue: Job[];
  public busy: boolean;
  isAuthenticated: boolean;
  userId?: number;
  heartbeatIntervalHandler?: NodeJS.Timeout;

  constructor(options: ClientConfig) {
    super();
    this.options = options;
    this.connection = new IggyConnection(options);
    this.busy = false;
    this.isAuthenticated = false;
    this._execQueue = [];
    this._init();
  };

  _init() {
    this.heartbeat(this.options.heartbeatInterval);
    this.connection.on('disconnected', async () => {
      this.isAuthenticated = false;
    });
  }

  async sendCommand(
    command: number,
    payload: Buffer,
    handleResponse = true,
    last = true
  ): Promise<CommandResponse> {

    if (!this.connection.connected)
      await this.connection.connect()

    if (!this.isAuthenticated && !UNLOGGED_COMMAND_CODE.includes(command))
      await this.authenticate(this.options.credentials);

    return new Promise(async (resolve, reject) => {
      if (last)
        this._execQueue.push({ command, payload, resolve, reject });
      else
        this._execQueue.unshift({ command, payload, resolve, reject });
      this._processQueue(handleResponse);
    });
  }

  async _processQueue(handleResponse = true): Promise<void> {
    if (this.busy)
      return;
    this.busy = true;
    while (this._execQueue.length > 0 && this.connection.socket.writable) {
      const next = this._execQueue.shift();
      if (!next) break;
      const { command, payload, resolve, reject } = next;
      try {
        resolve(await this._processNext(command, payload, handleResponse));
      } catch (err) {
        reject(err);
      }
    }
    this.busy = false;
    this.emit('finishQueue');
  }

  _processNext(
    command: number,
    payload: Buffer,
    handleResp = true
  ): Promise<CommandResponse> {
    const lastWrite = this.connection.writeCommand(command, payload);
    debug('==> writeCommand', lastWrite);
    return new Promise((resolve, reject) => {
      if (!lastWrite)
        return reject(new Error('failed to write to socket'));
      const errCb = (err: unknown) => reject(err);
      this.connection.once('error', errCb);
      this.connection.once('response', (resp) => {
        this.connection.removeListener('error', errCb);
        if (!handleResp) return resolve(resp);
        const r = handleResponse(resp);
        if (r.status !== 0) {
          return reject(responseError(command, r.status));
        }
        return resolve(r);
      });
    });
  }

  _failQueue(err: Error) {
    this._execQueue.forEach(({ reject }) => reject(err));
    this._execQueue = [];
  }

  async authenticate(creds: ClientCredentials) {
    const r = ('token' in creds) ?
      await this._authWithToken(creds) :
      await this._authWithPassword(creds);
    this.isAuthenticated = true;
    this.userId = r.userId;
    return this.isAuthenticated;
  }

  async _authWithPassword(creds: PasswordCredentials) {
    const pl = LOGIN.serialize(creds);
    const logr = await this.sendCommand(LOGIN.code, pl, true, false);
    return LOGIN.deserialize(logr);
  }

  async _authWithToken(creds: TokenCredentials) {
    const pl = LOGIN_WITH_TOKEN.serialize(creds);
    const logr = await this.sendCommand(LOGIN_WITH_TOKEN.code, pl, true, false);
    return LOGIN_WITH_TOKEN.deserialize(logr);
  }

  async ping() {
    const pl = PING.serialize();
    const pingR = await this.sendCommand(PING.code, pl, true);
    return PING.deserialize(pingR);
  }

  heartbeat(interval?: number) {
    if (!interval)
      return

    this.heartbeatIntervalHandler = setInterval(async () => {
      if (this.connection.connected) {
        debug(`sending hearbeat ping (interval: ${interval} ms)`);
        await this.ping()
      }
    }, interval);
  }

  getReadStream() {
    return this.connection.socket;
  }

  destroy() {
    if (this.heartbeatIntervalHandler)
      clearInterval(this.heartbeatIntervalHandler);
    return this.connection._destroy();
  }
};


export function getRawClient(options: ClientConfig): RawClient {
  return new CommandResponseStream(options);
}
