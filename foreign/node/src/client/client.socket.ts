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
//

import { EventEmitter } from 'node:events';
import type {
  ClientConfig,
  ClientCredentials, CommandResponse,
  PasswordCredentials, RawClient, TokenCredentials
} from '../client/client.type.js';
import type { BinaryRequestKind } from '../wire/command-set.js';
import { handleResponse } from './client.utils.js';
import { ResponseError, responseError } from '../wire/error.utils.js';
import { debug } from './client.debug.js';
import { IggyConnection } from './client.connection.js';
import { LOGIN, LOGIN_WITH_TOKEN, LOGOUT, PING } from '../wire/index.js';
import { GET_CLUSTER_METADATA } from '../wire/cluster/get-cluster-metadata.command.js';
import { COMMAND_CODE } from '../wire/command.code.js';
import {
  decodeVsrResponse,
  prepareVsrCommand,
  readRegisteredSession,
  VsrSession,
} from '../wire/vsr/index.js';

const VSR_RESPONSE_TIMEOUT_MS = 30_000;
const VSR_RETRY_INTERVAL_MS = 50;
const TRANSIENT_NOT_COMMITTED = 57;
const TRANSIENT_NOT_ACCEPTED = 58;

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
  /** Whether to parse the response */
  handleResponse: boolean,
  /** Execution model declared by a raw request */
  kind?: BinaryRequestKind,
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
  /** Server wire protocol used by this connection */
  readonly protocol;
  /** Client configuration */
  private options: ClientConfig;
  /** Underlying connection to the server */
  private connection: IggyConnection;
  /** Queue of pending command jobs */
  private _execQueue: Job[];
  /** Consensus session used by VSR framing */
  private vsrSession: VsrSession;
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
    this.protocol = options.protocol ?? 'classic';
    this.options = options;
    this.connection = new IggyConnection(options);
    this.busy = false;
    this.isAuthenticated = false;
    this._execQueue = [];
    this.vsrSession = new VsrSession();
    this._init();
  };

  /**
   * Initializes the stream by setting up heartbeat and connection event handlers.
   */
  _init() {
    this.heartbeat(this.options.heartbeatInterval);
    this.connection.on('disconnected', async () => {
      this.isAuthenticated = false;
      this.userId = undefined;
      this.vsrSession.reset();
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
    last = true,
    kind?: BinaryRequestKind
  ): Promise<CommandResponse> {

    if (!this.connection.connected)
      await this.connection.connect()

    if (this.options.protocol === 'vsr' &&
        isLoginCommand(command) &&
        this.isAuthenticated)
      await this.sendCommand(LOGOUT.code, LOGOUT.serialize(), true, false);

    if (!this.isAuthenticated && !this.isUnloggedCommand(command))
      await this.authenticate(this.options.credentials);

    return new Promise((resolve, reject) => {
      const job = { command, payload, handleResponse, kind, resolve, reject };
      if (last)
        this._execQueue.push(job);
      else
        this._execQueue.unshift(job);
      this._processQueue();
    });
  }

  /**
   * Processes queued commands sequentially.
   * Emits 'finishQueue' when all commands are processed.
   *
   */
  async _processQueue(): Promise<void> {
    if (this.busy)
      return;
    this.busy = true;
    while (this._execQueue.length > 0 && this.connection.socket.writable) {
      const next = this._execQueue.shift();
      if (!next) break;
      const { command, payload, handleResponse, kind, resolve, reject } = next;
      try {
        resolve(await this._processNext(command, payload, handleResponse, kind));
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
    handleResp = true,
    kind?: BinaryRequestKind
  ): Promise<CommandResponse> {
    if (this.options.protocol !== 'vsr')
      return this._processClassic(command, payload, handleResp);
    return this._processVsr(command, payload, handleResp, kind);
  }

  private async _processClassic(
    command: number,
    payload: Buffer,
    handleResp: boolean
  ): Promise<CommandResponse> {
    const response = await this._exchange(
      () => this.connection.writeCommand(command, payload)
    );
    if (!handleResp)
      return response as unknown as CommandResponse;
    const parsed = handleResponse(response);
    if (parsed.status !== 0)
      throw responseError(command, parsed.status);
    return parsed;
  }

  private async _processVsr(
    command: number,
    payload: Buffer,
    handleResp: boolean,
    kind?: BinaryRequestKind
  ): Promise<CommandResponse> {
    const prepared = prepareVsrCommand(command, payload);
    // Encode once so a transient replay preserves the request ID used by
    // server-side deduplication.
    const frame = this.vsrSession.encode(prepared.command, prepared.payload, kind);
    const deadline = Date.now() + VSR_RESPONSE_TIMEOUT_MS;
    let parsed: CommandResponse;
    while (true) {
      const remaining = deadline - Date.now();
      if (remaining <= 0)
        throw new Error(
          `timed out after ${VSR_RESPONSE_TIMEOUT_MS} ms waiting for VSR response`
        );
      const response = await this._exchange(
        () => this.connection.writeFrame(frame),
        remaining
      );
      if (!handleResp)
        return response as unknown as CommandResponse;
      try {
        parsed = decodeVsrResponse(response);
        break;
      } catch (error) {
        if (!(error instanceof ResponseError) ||
            !isTransientVsrError(error.errorCode))
          throw error;
        const retryDelay = Math.min(
          VSR_RETRY_INTERVAL_MS,
          Math.max(0, deadline - Date.now())
        );
        if (retryDelay === 0)
          throw error;
        await delay(retryDelay);
      }
    }

    if (prepared.command === COMMAND_CODE.LoginRegister ||
        prepared.command === COMMAND_CODE.LoginRegisterWithAccessToken) {
      this.vsrSession.bind(readRegisteredSession(parsed));
      this.isAuthenticated = true;
      this.userId = parsed.data.readUInt32LE(0);
    }
    if (prepared.command === COMMAND_CODE.LogoutUser) {
      this.isAuthenticated = false;
      this.userId = undefined;
      this.vsrSession.reset();
    }
    return parsed;
  }

  private _exchange(write: () => boolean, timeout?: number): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      let timeoutHandler: NodeJS.Timeout | undefined;
      const cleanup = () => {
        if (timeoutHandler)
          clearTimeout(timeoutHandler);
        this.connection.removeListener('error', errorCallback);
        this.connection.removeListener('disconnected', disconnectedCallback);
        this.connection.removeListener('response', responseCallback);
      };
      const errorCallback = (error: unknown) => {
        cleanup();
        reject(error);
      };
      const disconnectedCallback = () => {
        cleanup();
        reject(new Error('connection closed while waiting for response'));
      };
      const responseCallback = (response: Buffer) => {
        cleanup();
        resolve(response);
      };
      if (timeout !== undefined) {
        timeoutHandler = setTimeout(() => {
          cleanup();
          reject(new Error(`timed out after ${timeout} ms waiting for VSR response`));
        }, timeout);
      }
      this.connection.once('error', errorCallback);
      this.connection.once('disconnected', disconnectedCallback);
      this.connection.once('response', responseCallback);
      if (!write()) {
        cleanup();
        reject(new Error('failed to write to socket'));
      }
    });
  }

  private isUnloggedCommand(command: number): boolean {
    return UNLOGGED_COMMAND_CODE.includes(command) ||
      (this.options.protocol === 'vsr' &&
        command === COMMAND_CODE.GetClusterMetadata);
  }

  private async _ensureVsrLeader(): Promise<void> {
    for (let attempt = 0; attempt < 3; attempt += 1) {
      const response = await this._processNext(
        GET_CLUSTER_METADATA.code,
        GET_CLUSTER_METADATA.serialize()
      );
      const metadata = GET_CLUSTER_METADATA.deserialize(response);
      if (metadata.nodes.length <= 1)
        return;
      const leader = metadata.nodes.find(
        (node) => node.role === 'Leader' && node.status === 'Healthy'
      );
      if (!leader) {
        await delay(100);
        continue;
      }
      if (!this.connection.isConnectedTo(leader.ip, leader.endpoints.tcp)) {
        await this.connection.redirect(leader.ip, leader.endpoints.tcp);
        this.vsrSession.reset();
      }
      return;
    }
    throw new Error('VSR cluster has no healthy leader');
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
    if (this.options.protocol === 'vsr')
      await this._ensureVsrLeader();
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
        debug(`sending heartbeat ping (interval: ${interval} ms)`);
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

const isLoginCommand = (command: number): boolean =>
  command === COMMAND_CODE.LoginUser ||
  command === COMMAND_CODE.LoginWithAccessToken;

const delay = (milliseconds: number): Promise<void> =>
  new Promise((resolve) => setTimeout(resolve, milliseconds));

const isTransientVsrError = (errorCode: number): boolean =>
  errorCode === TRANSIENT_NOT_COMMITTED ||
  errorCode === TRANSIENT_NOT_ACCEPTED;
