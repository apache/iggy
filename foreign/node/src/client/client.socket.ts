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


import { Duplex } from 'node:stream';

import type {
  ClientConfig,
  ClientCredentials, CommandResponse,
  PasswordCredentials, RawClient, TokenCredentials
} from './client.type.js';

import { handleResponse, serializeCommand } from './client.utils.js';
import { responseError } from '../wire/error.utils.js';
import { LOGIN } from '../wire/session/login.command.js';
import { LOGIN_WITH_TOKEN } from '../wire/session/login-with-token.command.js';
import { debug } from './client.debug.js';
import { IggyTransport } from './client.transport.js';
import { PING } from '../wire/index.js';

const UNLOGGED_COMMAND_CODE = [
  PING.code,
  LOGIN.code,
  LOGIN_WITH_TOKEN.code
];

type WriteCb = ((error: Error | null | undefined) => void) | undefined

type Job = {
  command: number,
  payload: Buffer,
  resolve: (v: CommandResponse | PromiseLike<CommandResponse>) => void,
  reject: (e: unknown) => void
};


export class CommandResponseStream extends Duplex  {
  private options: ClientConfig;
  private transport: IggyTransport;
  private _readPaused: boolean;
  private _execQueue: Job[];
  public busy: boolean;
  isAuthenticated: boolean;
  userId?: number;

  constructor(options: ClientConfig) {
    super();
    this.options = options;
    this.transport = new IggyTransport(options);    
    this._readPaused = false;
    this.busy = false;
    this.isAuthenticated = false;
    this._execQueue = [];
    this.wrapTransport();
    this.heartbeat(options.heartbeatInterval);
  };
  
  _destroy() {
    this.transport.socket.destroy();
  }

  _read(size: number): void {
    this._readPaused = false;
    debug('stream#_read', size);
    setImmediate(this._onReadable.bind(this));
  }

  _write(chunk: Buffer, encoding: BufferEncoding | undefined, cb?: WriteCb) {
    return this.transport.socket.write(chunk, encoding, cb);
  };

  writeCommand(command: number, payload: Buffer): boolean {
    const cmd = serializeCommand(command, payload);
    return this.transport.socket.write(cmd);
  }

  sendCommand(
    command: number, payload: Buffer, handleResponse = true, last = true
  ): Promise<CommandResponse> {
    return new Promise((resolve, reject) => {
      if(last)
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
    while (this._execQueue.length > 0 && this.transport.socket.writable) {
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
    const lastWrite = this.writeCommand(command, payload);
    debug('==> writeCommand', lastWrite);
    return new Promise((resolve, reject) => {
      if(!lastWrite)
        return reject(new Error('failed to write to socket'));
      const errCb = (err: unknown) => reject(err);
      this.once('error', errCb);
      this.once('data', (resp) => {
        this.removeListener('error', errCb);
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

  getReadStream() {
    return this;
  }
  
  wrapTransport() {
    this.transport.on('connect', () => {
      debug('socket#connect event');
      this.emit('connect');
      if(this._execQueue.length > 0) {
        if(!this.isAuthenticated) {
          const needAuth = this._execQueue.some(
            q => !UNLOGGED_COMMAND_CODE.includes(q.command)
          );
          if(needAuth)
            this.authenticate(this.options.credentials);
        }
        this._processQueue();
      }
    });

    this.transport.on('close', async (hadError?: boolean) => {
      console.log(
        'socket#close',
        { hadError, connected: this.transport.connected },
      );
      this.emit('close');
    });

    this.transport.on('end', async () => {
      console.error('socket#end');
      this.emit('end');
    });
    
    this.transport.on('error', (err) => {
      console.error('socket#err', err);
      this._failQueue(err);
      this.emit('error', err);
    });

    this.transport.on('disconnected', async () => {
      this.isAuthenticated = false;
    });

    this.transport.on('drain', () => this.emit('drain'));

    this.transport.on(
      'lookup',
      (err, address, family, host) => this.emit('lookup', err, address, family, host)
    );
    this.transport.on('ready', () => this.emit('ready'));
    this.transport.on('timeout', () => this.emit('timeout'));

    // customize data events
    this.transport.on('readable', () => this._onReadable());
    return this.transport;
  }

  _onReadable() {
    while (!this._readPaused) {
      const head = this.transport.socket.read(8);
      if (!head || head.length === 0) return;
      if (head.length < 8) {
        this.transport.socket.unshift(head);
        return;
      }
      /** first chunk[4:8] hold response length */
      const responseSize = head.readUInt32LE(4);
      /** response has no payload (create/update/delete ops...) */
      if (responseSize === 0) {
        this.push(head);
        return;
      }

      const payload = this.transport.socket.read(responseSize);
      debug('payload', payload, responseSize, head.readUInt32LE(0));
      if (!payload) {
        this.transport.socket.unshift(head);
        return;
      }
      /** payload is incomplete, unshift until next read */
      if (payload.length < responseSize) {
        this.transport.socket.unshift(Buffer.concat([head, payload]));
        return;
      }

      const pushOk = this.push(Buffer.concat([head, payload]));
      /** consumer is slower than producer */
      if (!pushOk)
        this._readPaused = true;
    }
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
    if(!interval)
      return
    
    setInterval(async () => {
      if(this.transport.connected) {
        debug(`sending hearbeat ping (interval: ${interval} ms)`);
        await this.ping()
      }
    }, interval);
  }

  
};

export function getRawClient(options: ClientConfig): RawClient {
  return new CommandResponseStream(options);
}
