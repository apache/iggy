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
  PasswordCredentials, RawClient, SendCommandOptions,
  TokenCredentials
} from '../client/client.type.js';
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
  VsrEvictionError,
  VsrSession,
} from '../wire/vsr/index.js';
import { normalizeClientConfig } from './client.config.js';

const VSR_RESPONSE_TIMEOUT_MS = 30_000;
const VSR_RETRY_INTERVAL_MS = 50;
const LEADERLESS_WAIT_BUDGET_MS = 5_000;
const LEADERLESS_POLL_INTERVAL_MS = 250;
const MAX_LEADER_REDIRECTS = 3;
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
  /** Promise resolve function */
  resolve: (v: CommandResponse | PromiseLike<CommandResponse>) => void,
  /** Promise reject function */
  reject: (e: unknown) => void
};

type ExchangeState = {
  written: boolean
};

export class VsrResponseTimeoutError extends Error {
  constructor(timeout: number) {
    super(`timed out after ${timeout} ms waiting for VSR response`);
    this.name = 'VsrResponseTimeoutError';
    Object.setPrototypeOf(this, VsrResponseTimeoutError.prototype);
  }
}

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
  /** Consensus session used by VSR framing */
  private vsrSession: VsrSession;
  /** Shared authentication attempt for concurrent callers */
  private authenticationPromise?: Promise<boolean>;
  /** Whether a login is already being moved to the leader */
  private settlingLeader: boolean;
  /** How long a leaderless roster is polled before settling in place */
  private leaderlessWaitBudget: number;
  /** Delay between roster reads while the cluster elects */
  private leaderlessPollInterval: number;
  /** Calls that have acquired this stream but have not fully settled */
  private pendingSubmissions: number;
  /** Whether the stream is currently processing a command */
  public busy: boolean;
  /** Whether the client has been authenticated */
  isAuthenticated: boolean;
  /** Authenticated user ID */
  userId?: number;
  /** Heartbeat interval timer handle */
  heartbeatIntervalHandler?: NodeJS.Timeout;
  /** Whether a heartbeat ping is still awaiting its response */
  private heartbeatInFlight: boolean;

  /**
   * Creates a new CommandResponseStream.
   *
   * @param options - Client configuration
   */
  constructor(options: ClientConfig) {
    super();
    const normalizedConfig = normalizeClientConfig(options);
    this.options = normalizedConfig;
    this.connection = new IggyConnection(normalizedConfig);
    this.busy = false;
    this.isAuthenticated = false;
    this._execQueue = [];
    this.vsrSession = new VsrSession();
    this.authenticationPromise = undefined;
    this.settlingLeader = false;
    this.leaderlessWaitBudget = LEADERLESS_WAIT_BUDGET_MS;
    this.leaderlessPollInterval = LEADERLESS_POLL_INTERVAL_MS;
    this.pendingSubmissions = 0;
    this.heartbeatInFlight = false;
    this._init();
  };

  /**
   * Initializes the stream by setting up heartbeat and connection event handlers.
   */
  _init() {
    this.heartbeat(this.options.heartbeatInterval);
    this.connection.on('error', (error: Error) => {
      this._failQueue(error);
    });
    this.connection.on('eviction', (error: VsrEvictionError) => {
      this._resetSession();
      this.emit('eviction', error);
    });
    this.connection.on('disconnected', () => {
      this._resetSession();
      this._failQueue(
        new Error('connection closed before queued commands were sent')
      );
    });
  }

  /**
   * Sends a command to the server.
   * Automatically handles connection, authentication and leader settlement.
   *
   * @param command - Command code to send
   * @param payload - Command payload buffer
   * @param options - Response and queue options
   * @returns Promise resolving to the command response
   */
  async sendCommand(
    command: number,
    payload: Buffer,
    options: SendCommandOptions = {}
  ): Promise<CommandResponse> {
    this.pendingSubmissions += 1;
    try {
      const {
        handleResponse = true,
        last = true
      } = options;

      if (!this.connection.connected)
        await this.connection.connect()

      if (!this.isAuthenticated && !this.isUnloggedCommand(command))
        await this.authenticate(this.options.credentials);

      const response = await new Promise<CommandResponse>(
        (resolve, reject) => {
          const job = {
            command,
            payload,
            handleResponse,
            resolve,
            reject
          };
          if (last)
            this._execQueue.push(job);
          else
            this._execQueue.unshift(job);
          this._processQueue();
        });
      if (!isLoginCommand(command) || this.settlingLeader)
        return response;
      this.settlingLeader = true;
      try {
        const settled = await this._settleOnLeader(command, payload);
        return settled ?? response;
      } finally {
        this.settlingLeader = false;
      }
    } finally {
      this.pendingSubmissions -= 1;
      this._emitFinishQueue();
    }
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
      const { command, payload, handleResponse, resolve, reject } = next;
      try {
        resolve(await this._processNext(command, payload, handleResponse));
      } catch (err) {
        reject(err);
      }
    }
    if (this._execQueue.length > 0)
      this._failQueue(new Error('connection is not writable'));
    this.busy = false;
    this._emitFinishQueue();
  }

  private _emitFinishQueue(): void {
    if (this.pendingSubmissions === 0 &&
        !this.busy &&
        this._execQueue.length === 0)
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
    if (isLoginCommand(command) && this.isAuthenticated)
      return this._processVsrLogin(command, payload, handleResp);
    return this._processVsr(command, payload, handleResp);
  }

  private async _processVsrLogin(
    command: number,
    payload: Buffer,
    handleResp: boolean
  ): Promise<CommandResponse> {
    await this._processVsr(LOGOUT.code, LOGOUT.serialize(), true);
    return this._processVsr(command, payload, handleResp);
  }

  private async _processVsr(
    command: number,
    payload: Buffer,
    handleResp: boolean
  ): Promise<CommandResponse> {
    let requestWritten = false;
    try {
      const prepared = prepareVsrCommand(command, payload);
      // A transient retry must preserve all request identity fields.
      const frame = this.vsrSession.encode(prepared.command, prepared.payload);
      const deadline = Date.now() + VSR_RESPONSE_TIMEOUT_MS;
      let lastTransientError: ResponseError | undefined;
      let parsed: CommandResponse;
      while (true) {
        const remaining = deadline - Date.now();
        if (remaining <= 0) {
          if (lastTransientError)
            throw lastTransientError;
          throw new VsrResponseTimeoutError(VSR_RESPONSE_TIMEOUT_MS);
        }
        const exchangeState = { written: false };
        let response: Buffer;
        try {
          response = await this._exchange(
            () => this.connection.writeFrame(frame),
            remaining,
            exchangeState
          );
        } finally {
          requestWritten ||= exchangeState.written;
        }
        if (!handleResp)
          return response as unknown as CommandResponse;
        try {
          parsed = decodeVsrResponse(response, command);
          break;
        } catch (error) {
          if (!(error instanceof ResponseError) ||
              !isTransientVsrError(error.errorCode))
            throw error;
          lastTransientError = error;
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
        this._resetSession();
      }
      return parsed;
    } catch (error) {
      // Once bytes were handed to the socket, a local transport or decode
      // failure leaves the request outcome ambiguous. Register a fresh session
      // rather than replaying that request under a different client identity.
      if (!(error instanceof ResponseError) && requestWritten)
        this._resetSession();
      if (error instanceof VsrEvictionError)
        throw error;
      if (error instanceof ResponseError)
        throw responseError(command, error.errorCode);
      throw error;
    }
  }

  private _exchange(
    write: () => void,
    timeout?: number,
    state?: ExchangeState
  ): Promise<Buffer> {
    return new Promise((resolve, reject) => {
      let timeoutHandler: NodeJS.Timeout | undefined;
      const cleanup = () => {
        if (timeoutHandler)
          clearTimeout(timeoutHandler);
        this.connection.removeListener('error', errorCallback);
        this.connection.removeListener('disconnected', disconnectedCallback);
        this.connection.removeListener('eviction', evictionCallback);
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
      const evictionCallback = (error: VsrEvictionError) => {
        cleanup();
        reject(error);
      };
      const responseCallback = (response: Buffer) => {
        cleanup();
        resolve(response);
      };
      if (timeout !== undefined) {
        timeoutHandler = setTimeout(() => {
          cleanup();
          this.connection.abort();
          reject(new VsrResponseTimeoutError(timeout));
        }, timeout);
      }
      this.connection.once('error', errorCallback);
      this.connection.once('disconnected', disconnectedCallback);
      this.connection.once('eviction', evictionCallback);
      this.connection.once('response', responseCallback);
      try {
        write();
        if (state)
          state.written = true;
      } catch (error) {
        cleanup();
        reject(error);
      }
    });
  }

  // `GetClusterMetadata` is deliberately absent: the server auth-gates it,
  // so the client authenticates before reading the topology. A login dialed
  // at a backup still succeeds because the server forwards the register to
  // the primary.
  private isUnloggedCommand(command: number): boolean {
    return UNLOGGED_COMMAND_CODE.includes(command);
  }

  /**
   * Moves a freshly authenticated session to the cluster leader.
   *
   * Only the leader accepts replicated commands, and the roster read is
   * auth-gated, so the topology cannot be inspected before a login binds a
   * session. The redirect drops that session along with the socket, so the
   * login is replayed on the leader and its answer supersedes the one from the
   * node the client dialed. Leadership can move between the roster read and
   * the replay, so each freshly bound hop rechecks the roster under a bounded
   * redirect budget.
   *
   * @returns The leader's login response, or undefined when the client stays
   */
  private async _settleOnLeader(
    loginCommand: number,
    loginPayload: Buffer
  ): Promise<CommandResponse | undefined> {
    let settledResponse: CommandResponse | undefined;
    for (let redirects = 0; redirects < MAX_LEADER_REDIRECTS; redirects += 1) {
      const leader = await this._readLeaderEndpoint();
      if (!leader || this.connection.isConnectedTo(leader.host, leader.port))
        return settledResponse;
      await this.connection.redirect(leader.host, leader.port);
      settledResponse = await this.sendCommand(
        loginCommand,
        loginPayload,
        { last: false }
      );
    }
    debug(
      `leader settlement reached its ${MAX_LEADER_REDIRECTS}-hop budget, ` +
      'staying on the current node'
    );
    return settledResponse;
  }

  /**
   * Reads the cluster roster and picks the endpoint to settle on.
   *
   * Best effort: an unreadable roster, `Unauthenticated` included (the session
   * died between the login and this read), keeps the client on its current
   * node instead of failing a login that already succeeded.
   */
  private async _readLeaderEndpoint():
    Promise<{ host: string, port: number } | undefined> {
    // A cluster can be transiently leaderless: a restarted node cedes the
    // primaryship its stale view assigns it, and the roster reports no leader
    // until the peers' election completes. That window is roughly one heartbeat
    // timeout, so poll through it rather than settling on a replica that denies
    // every replicated command for its whole retry budget.
    const deadline = Date.now() + this.leaderlessWaitBudget;
    while (true) {
      // Reading without a session would re-enter authentication, which awaits
      // the very login this settlement runs inside of. The session can also die
      // between polls, so this holds for every pass, not just the first.
      if (!this.isAuthenticated)
        return undefined;
      try {
        // Queue the metadata fetch instead of writing directly: a bare write
        // would race an in-flight exchange and both would wake on the same
        // response event.
        const response = await this.sendCommand(
          GET_CLUSTER_METADATA.code,
          GET_CLUSTER_METADATA.serialize(),
          { last: false }
        );
        const metadata = GET_CLUSTER_METADATA.deserialize(response);
        // Every read feeds the redial candidates, leaderless ones included: a
        // roster with no leader still names where the nodes are.
        this.connection.rememberRoster(
          metadata.nodes
            .filter((node) => node.endpoints.tcp !== 0)
            .map((node) => ({ host: node.ip, port: node.endpoints.tcp }))
        );
        if (metadata.nodes.length <= 1)
          return undefined;
        const leader = metadata.nodes.find(
          (node) => node.role === 'Leader' && node.status === 'Healthy'
        );
        if (leader)
          return { host: leader.ip, port: leader.endpoints.tcp };
      } catch (error) {
        debug('cluster metadata is unreadable, staying on this node', error);
        return undefined;
      }
      if (Date.now() >= deadline) {
        debug(
          'cluster metadata named no healthy leader within ' +
          `${this.leaderlessWaitBudget} ms, staying on this node`
        );
        return undefined;
      }
      await delay(this.leaderlessPollInterval);
    }
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

  private _resetSession(): void {
    if (!this.isAuthenticated &&
        this.userId === undefined &&
        !this.vsrSession.hasActivity)
      return;
    this.isAuthenticated = false;
    this.userId = undefined;
    this.vsrSession.reset();
    this.emit('sessionReset');
  }

  hold(): () => void {
    this.pendingSubmissions += 1;
    let released = false;
    return () => {
      if (released)
        return;
      released = true;
      this.pendingSubmissions -= 1;
      this._emitFinishQueue();
    };
  }

  /**
   * Authenticates the client with the server.
   *
   * @param creds - Authentication credentials (token or password)
   * @returns True if authentication succeeded
   */
  async authenticate(creds: ClientCredentials): Promise<boolean> {
    if (this.isAuthenticated)
      return true;
    if (this.authenticationPromise)
      return this.authenticationPromise;

    this.authenticationPromise = this._authenticate(creds);
    try {
      return await this.authenticationPromise;
    } finally {
      this.authenticationPromise = undefined;
    }
  }

  private async _authenticate(creds: ClientCredentials): Promise<boolean> {
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
    const logr = await this.sendCommand(LOGIN.code, pl, { last: false });
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
    const logr = await this.sendCommand(
      LOGIN_WITH_TOKEN.code,
      pl,
      { last: false }
    );
    return LOGIN_WITH_TOKEN.deserialize(logr);
  }

  /**
   * Sends a ping command to the server.
   *
   * @returns Ping response
   */
  async ping() {
    const pl = PING.serialize();
    const pingR = await this.sendCommand(PING.code, pl);
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
      if (this.connection.connected && !this.heartbeatInFlight) {
        this.heartbeatInFlight = true;
        debug(`sending heartbeat ping (interval: ${interval} ms)`);
        try {
          await this.ping()
          this.emit('heartbeat');
        } catch (error) {
          debug('heartbeat ping failed', error);
        } finally {
          this.heartbeatInFlight = false;
        }
      }
    }, interval);
    // A pending heartbeat must not be the reason the process stays up: a script
    // that never calls destroy() would otherwise hang on exit.
    this.heartbeatIntervalHandler.unref();
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
