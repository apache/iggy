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
import type { Socket } from 'node:net';
import { createConnection } from 'node:net';
import { connect as TLSConnect } from 'node:tls';
import type { ClientConfig, TlsOption, TcpOption, ReconnectOption } from "./client.type.js"
import { serializeCommand } from './client.utils.js';
import { debug } from './client.debug.js';
import { DEFAULT_MAX_RESPONSE_FRAME_SIZE } from './client.config.js';
import { extractResponseFrames } from './client.frame.js';


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
 * Recreates a socket after a delay.
 * Used for reconnection attempts.
 *
 * @param option - Client configuration
 * @param timer - Delay in milliseconds before recreating
 * @returns Promise resolving to a new socket
 */
function recreate(option: ClientConfig, timer = 1000): Promise<Socket> {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(getTransport(option));
    }, timer);
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
  /** Whether waiting for more data to complete a response */
  private waitingResponseEnd: boolean;
  /** Reconnection configuration */
  private reconnectOption: ReconnectOption;
  /** Number of reconnection attempts made */
  private reconnectCount: number;
  /** Shared promise for concurrent callers waiting on one connection attempt */
  private connectPromise?: Promise<this>;

  /** Buffer for incomplete response data */
  private readBuffers: Buffer;

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
    this.waitingResponseEnd = false;
    this.reconnectOption = { ...DefaultReconnectOption, ...config.reconnect };
    this.reconnectCount = 0;
    this.connectPromise = undefined;
    this.readBuffers = Buffer.allocUnsafe(0);
    this.socket = this._installSocket(getTransport(config));
  }

  /**
   * Attaches the lifecycle listeners exactly once per socket instance.
   * Attaching them in `connect()` would stack duplicate handlers whenever a
   * failed attempt is retried on the same socket.
   */
  private _installSocket(socket: Socket): Socket {
    socket.on('data', this._onData.bind(this));

    socket.on('error', (err: SocketError) => {
      debug('socket/error event', err, err.code, this.ending);
      if (this.ending && (err?.code === 'ECONNRESET' || err?.code === 'EPIPE'))
        return
      this.emit('error', err);
    });

    socket.once('close', (hadError?: boolean) => {
      debug('socket/close event', hadError);
      this.connected = false;
      this.connecting = false;
      this.connectPromise = undefined;
      this.emit('disconnected', hadError);
      if (!this.ending)
        void this.reconnect();
    });
    return socket;
  }

  /**
   * Establishes the connection to the server.
   *
   * @returns Promise that resolves when connected
   */
  connect(): Promise<this> {
    if (this.connected)
      return Promise.resolve(this);
    if (this.connectPromise)
      return this.connectPromise;
    // A reconnect is waiting out its backoff; the current socket is dead and
    // waiting on it would never settle.
    if (this.connecting)
      return Promise.reject(
        new Error('connection attempt already in progress')
      );
    if (this.socket.destroyed)
      this.socket = this._installSocket(getTransport(this.config));

    this.connecting = true;

    this.connectPromise = new Promise<this>((resolve, reject) => {
      const rejectConnect = (error: Error) => {
        this.socket.removeListener('connect', resolveConnect);
        this.connecting = false;
        this.connectPromise = undefined;
        reject(error);
      };
      const resolveConnect = () => {
        this.socket.removeListener('error', rejectConnect);
        debug('socket/connect event');
        this.connected = true;
        this.connecting = false;
        this.reconnectCount = 0;
        this.connectPromise = undefined;
        this.emit('connect');
        resolve(this);
      };
      this.socket.once('error', rejectConnect);
      this.socket.once('connect', resolveConnect);
    });
    return this.connectPromise;
  }

  /**
   * Attempts to reconnect to the server.
   * Respects maxRetries limit and emits error when exceeded.
   *
   * @param err - Optional error that triggered the reconnection
   */
  async reconnect(err?: Error) {
    if (this.ending || this.connected || this.connecting)
      return;

    const { enabled, interval, maxRetries } = this.reconnectOption
    debug(
      'reconnect# event/reconnect?', {
      reconnect: { enabled, interval, maxRetries },
      count: this.reconnectCount,
      lastError: err
    }
    );

    if (!enabled || this.reconnectCount >= maxRetries) {
      debug(`reconnect reached maxRetries of ${maxRetries}`, err);
      return this.emit(
        'error',
        new Error(
          `reconnect maxRetries exceeded (count: ${this.reconnectCount})`,
          { cause: err }
        ));
    }

    /** recreate socket */
    this.connecting = true;
    this.reconnectCount += 1;
    this.socket = this._installSocket(await recreate(this.config, interval));
    this.connecting = false;
    try {
      await this.connect();
    } catch (error) {
      debug('reconnect attempt failed', error);
    }
  }

  async redirect(host: string, port: number) {
    this.socket.removeAllListeners();
    this.socket.destroy();
    this.connected = false;
    this.connecting = false;
    this.connectPromise = undefined;
    this._endResponseWait();
    // The old socket's close handler was just detached, so surface the drop
    // to any in-flight exchange and queued work ourselves.
    this.emit('disconnected', false);
    this.config.options = { ...this.config.options, host, port };
    this.socket = this._installSocket(getTransport(this.config));
    await this.connect();
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
    this.socket.destroy();
  }

  /**
   * Clears the response buffer and resets the waiting state.
   */
  _endResponseWait() {
    this.readBuffers = Buffer.allocUnsafe(0);
    this.waitingResponseEnd = false;
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
      this.waitingResponseEnd
    );

    const buffered = this.waitingResponseEnd && this.readBuffers.length > 0
      ? Buffer.concat([this.readBuffers, data])
      : data;

    try {
      const extracted = extractResponseFrames(
        this.config.protocol ?? 'classic',
        buffered,
        this.config.maxResponseFrameSize ?? DEFAULT_MAX_RESPONSE_FRAME_SIZE
      );
      this.readBuffers = extracted.remainder;
      this.waitingResponseEnd = extracted.remainder.length > 0;
      for (const response of extracted.frames)
        this.emit('response', response);
    } catch (error) {
      this._endResponseWait();
      this.emit(
        'error',
        error instanceof Error ? error : new Error(String(error))
      );
      this.socket.destroy();
    }
  }

  /**
   * Writes a command to the socket.
   *
   * @param command - Command code
   * @param payload - Command payload
   * @returns True if the write was successful
   */
  writeCommand(command: number, payload: Buffer): void {
    const cmd = serializeCommand(command, payload);
    this.socket.write(cmd);
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
