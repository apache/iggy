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


/**
 * Command codes that can be executed without authentication.
 */
const UNLOGGED_COMMAND_CODE = [
  PING.code,
  LOGIN.code,
  LOGIN_WITH_TOKEN.code
];

/**
 * Represents a queued command job waiting to be executed.
 */
type Job = {
  /** Command code */
  command: number,
  /** Command payload */
  payload: Buffer,
  /** Promise resolve function */
  resolve: (v: CommandResponse | PromiseLike<CommandResponse>) => void,
  /** Promise reject function */
  reject: (e: unknown) => void
};


/**
 * Manages command execution and response handling for the Iggy server.
 * Implements command queuing, authentication, and heartbeat functionality.
 */
export class CommandResponseStream extends EventEmitter {
  /** Client configuration */
  private options: ClientConfig;
  /** Underlying connection to the server */
  private connection: IggyConnection;
  /** Queue of pending command jobs */
  private _execQueue: Job[];
  /** Whether the stream is currently processing a command */
  public busy: boolean;
  /** Whether the client has been authenticated */
  isAuthenticated: boolean;
  /** Authenticated user ID */
  userId?: number;
  /** Heartbeat interval timer handle */
  heartbeatIntervalHandler?: NodeJS.Timeout;

  /**
   * Creates a new CommandResponseStream.
   *
   * @param options - Client configuration
   */
  constructor(options: ClientConfig) {
    super();
    this.options = options;
    this.connection = new IggyConnection(options);
    this.busy = false;
    this.isAuthenticated = false;
    this._execQueue = [];
    this._init();
  };

  /**
   * Initializes the stream by setting up heartbeat and connection event handlers.
   */
  _init() {
    this.heartbeat(this.options.heartbeatInterval);
    this.connection.on('disconnected', async () => {
      this.isAuthenticated = false;
    });
  }

  /**
   * Sends a command to the server.
   * Automatically handles connection and authentication if needed.
   *
   * @param command - Command code to send
   * @param payload - Command payload buffer
   * @param handleResponse - Whether to parse the response (default: true)
   * @param last - Whether to add to end of queue (default: true)
   * @returns Promise resolving to the command response
   */
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

    return new Promise((resolve, reject) => {
      if (last)
        this._execQueue.push({ command, payload, resolve, reject });
      else
        this._execQueue.unshift({ command, payload, resolve, reject });
      this._processQueue(handleResponse);
    });
  }

  /**
   * Processes queued commands sequentially.
   * Emits 'finishQueue' when all commands are processed.
   *
   * @param handleResponse - Whether to parse responses
   */
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

  /**
   * Processes a single command by writing it to the connection and waiting for response.
   *
   * @param command - Command code
   * @param payload - Command payload
   * @param handleResp - Whether to parse the response
   * @returns Promise resolving to the command response
   */
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

  /**
   * Fails all queued commands with the given error.
   *
   * @param err - Error to reject all queued commands with
   */
  _failQueue(err: Error) {
    this._execQueue.forEach(({ reject }) => reject(err));
    this._execQueue = [];
  }

  /**
   * Authenticates the client with the server.
   *
   * @param creds - Authentication credentials (token or password)
   * @returns True if authentication succeeded
   */
  async authenticate(creds: ClientCredentials) {
    const r = ('token' in creds) ?
      await this._authWithToken(creds) :
      await this._authWithPassword(creds);
    this.isAuthenticated = true;
    this.userId = r.userId;
    return this.isAuthenticated;
  }

  /**
   * Authenticates using username and password.
   *
   * @param creds - Password credentials
   * @returns Login response with user ID
   */
  async _authWithPassword(creds: PasswordCredentials) {
    const pl = LOGIN.serialize(creds);
    const logr = await this.sendCommand(LOGIN.code, pl, true, false);
    return LOGIN.deserialize(logr);
  }

  /**
   * Authenticates using a token.
   *
   * @param creds - Token credentials
   * @returns Login response with user ID
   */
  async _authWithToken(creds: TokenCredentials) {
    const pl = LOGIN_WITH_TOKEN.serialize(creds);
    const logr = await this.sendCommand(LOGIN_WITH_TOKEN.code, pl, true, false);
    return LOGIN_WITH_TOKEN.deserialize(logr);
  }

  /**
   * Sends a ping command to the server.
   *
   * @returns Ping response
   */
  async ping() {
    const pl = PING.serialize();
    const pingR = await this.sendCommand(PING.code, pl, true);
    return PING.deserialize(pingR);
  }

  /**
   * Starts sending periodic heartbeat pings to keep the connection alive.
   *
   * @param interval - Heartbeat interval in milliseconds
   */
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

  /**
   * Returns the underlying socket as a readable stream.
   *
   * @returns The connection socket
   */
  getReadStream() {
    return this.connection.socket;
  }

  /**
   * Destroys the stream and cleans up resources.
   * Stops heartbeat and destroys the connection.
   */
  destroy() {
    if (this.heartbeatIntervalHandler)
      clearInterval(this.heartbeatIntervalHandler);
    return this.connection._destroy();
  }
};


/**
 * Creates a new RawClient instance.
 *
 * @param options - Client configuration
 * @returns RawClient instance
 */
export function getRawClient(options: ClientConfig): RawClient {
  return new CommandResponseStream(options);
}
