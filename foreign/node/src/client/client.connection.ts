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

import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createConnection } from 'node:net';
import { connect as TLSConnect } from 'node:tls';
import type { ClientConfig, TlsOption, TcpOption, ReconnectOption } from "./client.type.js"
import { debug } from './client.debug.js';
import { DEFAULT_MAX_RESPONSE_FRAME_SIZE } from './client.config.js';
import {
  ProtocolFrameError,
  ResponseFrameDecoder
} from './client.frame.js';
import { Command, peekCommand } from '../wire/vsr/header.js';
import { evictionError } from '../wire/vsr/reply.js';


/**
 * Creates a TCP socket connection.
 *
 * @param options - TCP connection options
 * @returns TCP socket
 */
const createTcpSocket = (options: TcpOption): Socket => {
  return createConnection(options);
};

/**
 * Creates a TLS socket connection.
 *
 * @param options - TLS connection options including port
 * @returns TLS socket
 */
const createTlsSocket = ({ port, ...options }: TlsOption): Socket => {
  const socket = TLSConnect(port, options);
  return socket;
};

/**
 * Creates a socket based on the transport type in the configuration.
 *
 * @param config - Client configuration with transport type
 * @returns Socket for the specified transport
 */
const getTransport = (config: ClientConfig): Socket => {
  const { transport, options } = config;
  switch (transport) {
    case 'TLS': return createTlsSocket(options);
    case 'TCP':
    default:
      return createTcpSocket(options);
  }
};

/**
 * Default reconnection settings.
 * Attempts reconnection every 5 seconds, up to 12 times.
 */
const DefaultReconnectOption: ReconnectOption = {
  enabled: true,
  interval: 5 * 1000,
  maxRetries: 12
}

/**
 * Waits before a reconnection attempt.
 *
 * @param timer - Delay in milliseconds before recreating
 * @returns Promise resolving after the delay
 */
function waitForReconnect(timer = 1000): Promise<void> {
  return new Promise((resolve) => {
    setTimeout(resolve, timer);
  });
}

/** Socket error with optional error code */
type SocketError = Error & { code?: string };

/**
 * Manages the low-level TCP/TLS connection to the Iggy server.
 * Handles connection lifecycle, reconnection, and data buffering.
 */
export class IggyConnection extends EventEmitter {
  /** Client configuration */
  public config: ClientConfig
  /** Underlying socket connection */
  public socket: Socket;

  /** Whether the connection is established */
  public connected: boolean;
  /** Whether a connection attempt is in progress */
  public connecting: boolean;
  /** Whether the connection is being intentionally closed */
  public ending: boolean;
  /** Reconnection configuration */
  private reconnectOption: ReconnectOption;
  /** Number of reconnection attempts made */
  private reconnectCount: number;
  /** Shared promise for concurrent callers waiting on one connection attempt */
  private connectPromise?: Promise<this>;
  /** Shared promise for callers waiting on automatic reconnection */
  private reconnectPromise?: Promise<this>;
  /** Endpoint the client was configured with, kept across leader redirects */
  private readonly seedOptions: ClientConfig['options'];
  /**
   * Every node the roster named on the last read, kept as redial candidates.
   * A node dies together with its address, and the roster is unreachable
   * exactly when it is needed, so it has to have been remembered while the
   * connection was still healthy.
   */
  private rosterEndpoints: { host: string, port: number }[];

  /** Incremental response frame decoder */
  private responseDecoder: ResponseFrameDecoder;

  /**
   * Creates a new IggyConnection.
   *
   * @param config - Client configuration
   */
  constructor(config: ClientConfig) {
    super();
    this.config = config;
    this.connected = false;
    this.connecting = false;
    this.ending = false;
    this.reconnectOption = { ...DefaultReconnectOption, ...config.reconnect };
    this.seedOptions = { ...config.options };
    this.rosterEndpoints = [];
    this.reconnectCount = 0;
    this.connectPromise = undefined;
    this.reconnectPromise = undefined;
    this.responseDecoder = new ResponseFrameDecoder(
      config.maxResponseFrameSize ?? DEFAULT_MAX_RESPONSE_FRAME_SIZE
    );
    this.socket = this._installSocket(getTransport(config));
  }

  /**
   * Attaches the lifecycle listeners exactly once per socket instance.
   * Attaching them in `connect()` would stack duplicate handlers whenever a
   * failed attempt is retried on the same socket.
   */
  private _installSocket(socket: Socket): Socket {
    socket.on('data', (data) => {
      if (this.socket !== socket)
        return;
      if (!Buffer.isBuffer(data)) {
        this.emit(
          'error',
          new ProtocolFrameError('socket returned text instead of binary data')
        );
        socket.destroy();
        return;
      }
      this._onData(data);
    });

    socket.on('error', (err: SocketError) => {
      if (this.socket !== socket)
        return;
      debug('socket/error event', err, err.code, this.ending);
      if (this.ending && (err?.code === 'ECONNRESET' || err?.code === 'EPIPE'))
        return
      this.emit('error', err);
    });

    socket.once('connect', () => {
      if (this.socket !== socket)
        return;
      debug('socket/connect event');
      this.connected = true;
      this.connecting = false;
      this.reconnectCount = 0;
      this.emit('connect');
    });

    socket.once('close', (hadError?: boolean) => {
      if (this.socket !== socket)
        return;
      debug('socket/close event', hadError);
      this.connected = false;
      this.connecting = false;
      this.connectPromise = undefined;
      this._endResponseWait();
      this.emit('disconnected', hadError);
      if (!this.ending)
        void this.reconnect().catch(() => undefined);
    });
    return socket;
  }

  /**
   * Establishes the connection to the server.
   *
   * @returns Promise that resolves when connected
   */
  connect(): Promise<this> {
    if (this.ending)
      return Promise.reject(new Error('connection is closed'));
    if (this.connected)
      return Promise.resolve(this);
    if (this.reconnectPromise)
      return this.reconnectPromise;
    if (this.connectPromise)
      return this.connectPromise;
    if (this.socket.destroyed)
      this.socket = this._installSocket(getTransport(this.config));

    this.connecting = true;
    const socket = this.socket;
    const connectPromise = this._waitForConnection(socket);
    this.connectPromise = connectPromise;
    const clearConnectPromise = () => {
      if (this.connectPromise === connectPromise)
        this.connectPromise = undefined;
    };
    void connectPromise.then(clearConnectPromise, clearConnectPromise);
    return connectPromise;
  }

  private _waitForConnection(socket: Socket): Promise<this> {
    return new Promise<this>((resolve, reject) => {
      const cleanup = () => {
        socket.removeListener('connect', resolveConnect);
        socket.removeListener('error', rejectConnect);
        socket.removeListener('close', rejectClosed);
      };
      const rejectConnect = (error: Error) => {
        cleanup();
        reject(error);
      };
      const rejectClosed = () => {
        rejectConnect(new Error('connection closed before it was established'));
      };
      const resolveConnect = () => {
        cleanup();
        resolve(this);
      };
      socket.once('error', rejectConnect);
      socket.once('close', rejectClosed);
      socket.once('connect', resolveConnect);
    });
  }

  /**
   * Attempts to reconnect to the server.
   * Respects maxRetries limit and emits error when exceeded.
   *
   * @param err - Optional error that triggered the reconnection
   */
  async reconnect(err?: Error): Promise<this | undefined> {
    if (this.ending || this.connected)
      return;
    if (this.reconnectPromise)
      return this.reconnectPromise;

    const { enabled, interval, maxRetries } = this.reconnectOption
    debug(
      'reconnect# event/reconnect?', {
      reconnect: { enabled, interval, maxRetries },
      count: this.reconnectCount,
      lastError: err
    }
    );

    const reconnectPromise = this._reconnectUntilConnected(
      enabled,
      interval,
      maxRetries,
      err
    );
    this.reconnectPromise = reconnectPromise;
    try {
      return await reconnectPromise;
    } catch (error) {
      if (!this.ending)
        this.emit('error', error);
      return;
    } finally {
      if (this.reconnectPromise === reconnectPromise)
        this.reconnectPromise = undefined;
      this.connecting = false;
    }
  }

  private async _reconnectUntilConnected(
    enabled: boolean,
    interval: number,
    maxRetries: number,
    initialError?: Error
  ): Promise<this> {
    let lastError = initialError;
    let expectedSocket = this.socket;
    while (enabled && this.reconnectCount < maxRetries) {
      this.connecting = true;
      this.reconnectCount += 1;
      await waitForReconnect(interval);
      if (this.ending)
        throw new Error('connection is closed', { cause: lastError });
      // A redirect may replace the socket at any point. Defer to the active
      // connection instead of dialing the superseded endpoint.
      if (this.connected || this.socket !== expectedSocket)
        return this.connect();

      // Every endpoint gets its turn inside one attempt, so a full pass over
      // the cluster costs one retry rather than one per endpoint: a pass that
      // stopped at the first refusal would never reach the survivors of a
      // client configured for a single retry.
      for (const options of this._redialCandidates()) {
        const socket = this._installSocket(
          getTransport({ ...this.config, options })
        );
        this.socket = socket;
        expectedSocket = socket;
        try {
          await this._waitForConnection(socket);
          if (this.socket !== socket)
            return this.connect();
          this.config.options = options;
          return this;
        } catch (error) {
          lastError = error instanceof Error
            ? error
            : new Error(String(error));
          debug('reconnect attempt failed', lastError);
        }
      }
    }

    debug(`reconnect reached maxRetries of ${maxRetries}`, lastError);
    throw new Error(
      `reconnect maxRetries exceeded (count: ${this.reconnectCount})`,
      { cause: lastError }
    );
  }

  /**
   * Records the cluster roster as redial candidates.
   *
   * Replaced wholesale rather than merged: the roster is the cluster's own
   * answer about where its nodes are, so a node it dropped stops being
   * dialed. The configured seed is kept separately and outlives it.
   */
  rememberRoster(endpoints: { host: string, port: number }[]): void {
    if (endpoints.length === 0)
      return;
    this.rosterEndpoints = endpoints;
  }

  /**
   * Endpoints a redial rotates through, likeliest first: where the client
   * currently is, the endpoint it was configured with, then the roster it
   * learned while connected. After a leader redirect the current endpoint may
   * die with the leader, and the rest of the list is the way back to the
   * cluster. Duplicates are dropped, so an endpoint the roster merely spells
   * differently does not earn a second attempt.
   */
  _redialCandidates(): ClientConfig['options'][] {
    const candidates = [this.config.options];
    const known = [
      this.seedOptions,
      ...this.rosterEndpoints.map(
        ({ host, port }) => ({ ...this.config.options, host, port })
      )
    ];
    for (const candidate of known) {
      const duplicate = candidates.some(
        (known) => known.port === candidate.port &&
          normalizeHost(known.host) === normalizeHost(candidate.host)
      );
      if (!duplicate)
        candidates.push(candidate);
    }
    return candidates;
  }

  async redirect(host: string, port: number) {
    const redirectedOptions = { ...this.config.options, host, port };
    const redirectedConfig = {
      ...this.config,
      options: redirectedOptions
    };
    // Destroying the old socket settles any dial still waiting on it. Its
    // lifecycle listeners stay attached but go inert once the socket is
    // replaced below, so surface the drop to in-flight exchanges ourselves.
    this.socket.destroy();
    this.connected = false;
    this.connecting = false;
    this.connectPromise = undefined;
    this.reconnectPromise = undefined;
    this._endResponseWait();
    this.socket = this._installSocket(getTransport(redirectedConfig));
    this.emit('disconnected', false);
    await this.connect();
    this.config.options = redirectedOptions;
  }

  abort(): void {
    this._endResponseWait();
    this.socket.destroy();
  }

  isConnectedTo(host: string, port: number): boolean {
    const target = normalizeHost(host);
    if (this.socket.remotePort === port &&
        normalizeHost(this.socket.remoteAddress) === target)
      return true;
    // A roster may advertise a DNS name while the socket reports a resolved
    // address; falling back to the configured endpoint avoids a redirect to
    // the peer the client is already connected to.
    return this.config.options.port === port &&
      normalizeHost(this.config.options.host) === target;
  }

  /**
   * Destroys the connection and marks it as ending.
   */
  _destroy() {
    this.ending = true;
    this._endResponseWait();
    this.socket.destroy();
  }

  /**
   * Clears the response buffer and resets the waiting state.
   */
  _endResponseWait() {
    this.responseDecoder.clear();
  }

  /**
   * Handles incoming data from the socket.
   * Buffers incomplete responses and emits complete ones.
   *
   * @param data - Incoming data buffer
   */
  _onData(data: Buffer) {
    debug(
      'ONDATA',
      typeof data,
      Buffer.isBuffer(data),
      data?.length,
      this.responseDecoder.hasBufferedData
    );

    try {
      for (const response of this.responseDecoder.push(data)) {
        if (peekCommand(response) === Command.Eviction)
          this.emit('eviction', evictionError(response));
        else
          this.emit('response', response);
      }
    } catch (error) {
      this._endResponseWait();
      this.emit(
        'error',
        error instanceof Error ? error : new Error(String(error))
      );
      this.socket.destroy();
    }
  }

  writeFrame(frame: Buffer): void {
    this.socket.write(frame);
  }
}

const normalizeHost = (host?: string): string => {
  const normalized = (host ?? '').toLowerCase().replace(/^::ffff:/, '');
  return normalized === 'localhost' || normalized === '::1'
    ? '127.0.0.1'
    : normalized;
};
