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
import { createConnection, isIP } from 'node:net';
import { connect as TLSConnect } from 'node:tls';
import { readFileSync } from 'node:fs';
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
  const { caFile, ...tlsOptions } = options;
  if (caFile !== undefined)
    tlsOptions.ca = readCaCertificate(caFile);
  // An SNI-routing terminator needs a server name in the ClientHello;
  // IP literals are never sent as SNI, matching the Rust SDK's fallback.
  if (typeof tlsOptions.host === 'string' &&
      tlsOptions.servername === undefined &&
      isIP(tlsOptions.host) === 0)
    tlsOptions.servername = tlsOptions.host;
  return TLSConnect(port, tlsOptions);
};

// A missing or unreadable CA file is a configuration error rather than a
// transient I/O failure, hence the TypeError instead of the raw ENOENT.
const readCaCertificate = (caFile: string): Buffer => {
  try {
    return readFileSync(caFile);
  } catch {
    throw new TypeError(`cannot read tls_ca_file "${caFile}"`);
  }
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

/** One node of the cluster, as a redial candidate. */
export type Endpoint = { host: string, port: number };

/**
 * Bound on one dial while other endpoints are queued behind it. Neither the
 * connect nor the TLS handshake has a deadline of its own, so a node whose syns
 * are dropped -- or one that accepts TCP and never answers the ClientHello --
 * would hold the whole pass. Matches the Rust SDK.
 */
const FAILOVER_DIAL_TIMEOUT_MS = 2_000;

/**
 * Default reconnection settings.
 *
 * One retry is one full pass over every endpoint the client knows, so this is
 * twelve passes rather than twelve dials, waiting 5 seconds between them. The
 * first pass runs at once when more than one endpoint is known: the node just
 * lost may be gone for good, and pausing before dialing a survivor only pushes
 * the failover past the interval a caller is willing to wait.
 */
const DefaultReconnectOption: ReconnectOption = {
  enabled: true,
  interval: 5 * 1000,
  maxRetries: 12
}

/**
 * Waits before a reconnection attempt.
 *
 * The timer is unref'd: a queued command fails fast on 'disconnected', so
 * the retry loop is background work that must not keep the process alive.
 *
 * @param interval - Delay in milliseconds before dialing again
 */
const waitForReconnect = (interval: number): Promise<void> =>
  new Promise<void>((resolve) => {
    const timeout = setTimeout(resolve, interval);
    (timeout as NodeJS.Timeout).unref();
  });

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
  /**
   * Whether the socket is being replaced by a deliberate leader redirect
   * rather than lost. The drop looks the same from the outside, but nothing a
   * caller submitted is in doubt: work waiting to be sent belongs on the node
   * the client moves to, not in an error.
   */
  public redirecting: boolean;
  /** Reconnection configuration */
  private reconnectOption: ReconnectOption;
  /**
   * Number of passes made over the known endpoints. One pass dials the
   * endpoint the client is on, the endpoint it was configured with, and every
   * node the roster named.
   */
  private reconnectCount: number;
  /** Shared promise for concurrent callers waiting on one connection attempt */
  private connectPromise?: Promise<this>;
  /** Shared promise for callers waiting on automatic reconnection */
  private reconnectPromise?: Promise<this>;
  /** Avoids duplicating the constructor's deferred attempt notification */
  private announcedAttemptSocket?: Socket;
  private initialAttemptNotificationPending: boolean;
  /** Endpoint the client was configured with, kept across leader redirects */
  private readonly seedOptions: ClientConfig['options'];
  /**
   * Every node the roster named on the last read, kept as redial candidates.
   * A node dies together with its address, and the roster is unreachable
   * exactly when it is needed, so it has to have been remembered while the
   * connection was still healthy.
   */
  private rosterEndpoints: Endpoint[];

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
    this.redirecting = false;
    this.reconnectOption = { ...DefaultReconnectOption, ...config.reconnect };
    this.seedOptions = { ...config.options };
    this.rosterEndpoints = [];
    this.reconnectCount = 0;
    this.connectPromise = undefined;
    this.reconnectPromise = undefined;
    this.announcedAttemptSocket = undefined;
    this.initialAttemptNotificationPending = true;
    this.responseDecoder = new ResponseFrameDecoder(
      config.maxResponseFrameSize ?? DEFAULT_MAX_RESPONSE_FRAME_SIZE
    );
    this.socket = this._installSocket(getTransport(config));
    const initialSocket = this.socket;
    queueMicrotask(() => {
      if (!this.initialAttemptNotificationPending)
        return;
      this.initialAttemptNotificationPending = false;
      this._announceConnectionAttempt(initialSocket);
    });
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
      if (this.announcedAttemptSocket === socket)
        this.announcedAttemptSocket = undefined;
      debug('socket/error event', err, err.code, this.ending);
      if (this.ending && (err?.code === 'ECONNRESET' || err?.code === 'EPIPE'))
        return
      this.emit('error', err);
    });

    // The readiness event, not 'connect': on TLS the socket is only usable
    // once the handshake completes, and writing a request before that would
    // announce a connection the peer has not agreed to yet.
    socket.once(this._readyEvent(), () => {
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
      this.announcedAttemptSocket = undefined;
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
    // Bounded here too when the client knows somewhere else to go: this dial
    // is not part of a pass, so an endpoint that never becomes usable would
    // hold it with no timer of its own and the redial pass would never start.
    const connectPromise = this._dialWithin(
      socket,
      this._redialCandidates().length > 1
    );
    this.connectPromise = connectPromise;
    const clearConnectPromise = () => {
      if (this.connectPromise === connectPromise)
        this.connectPromise = undefined;
    };
    void connectPromise.then(clearConnectPromise, clearConnectPromise);
    return connectPromise;
  }

  /**
   * Waits for one dial, bounded while other endpoints are queued behind it.
   *
   * A socket has no connect deadline of its own and there is no 'timeout'
   * listener on it, so a node whose syns are dropped holds the pass for the
   * whole OS connect timeout -- and it leads every pass, because the current
   * endpoint only moves on success. The bound covers the TLS handshake too,
   * which is what `_readyEvent()` waits for and has no deadline of its own
   * either. It matches the Rust, Go and C# SDKs'.
   */
  private async _dialWithin(socket: Socket, bounded: boolean): Promise<this> {
    this.initialAttemptNotificationPending = false;
    this._announceConnectionAttempt(socket);
    if (!bounded)
      return this._waitForConnection(socket);

    let expire: NodeJS.Timeout | undefined;
    const bound = new Promise<never>((_resolve, reject) => {
      expire = setTimeout(() => {
        // Destroying it makes the pending dial settle and releases the handle;
        // left alone it would keep the event loop alive.
        socket.destroy();
        reject(new Error(
          `dial exceeded ${FAILOVER_DIAL_TIMEOUT_MS}ms`
        ));
      }, FAILOVER_DIAL_TIMEOUT_MS);
      expire.unref?.();
    });

    try {
      return await Promise.race([this._waitForConnection(socket), bound]);
    } finally {
      clearTimeout(expire);
    }
  }

  private _announceConnectionAttempt(socket: Socket): void {
    if (this.announcedAttemptSocket === socket ||
        this.socket !== socket ||
        this.connected ||
        this.ending)
      return;
    this.announcedAttemptSocket = socket;
    this.emit('connecting');
  }

  /**
   * The event that says a socket can carry a request.
   *
   * On TLS that is 'secureConnect', not 'connect': the latter fires as soon as
   * the TCP handshake completes, so waiting on it would treat a peer that
   * never answers the ClientHello as connected and leave the handshake with no
   * deadline at all.
   */
  private _readyEvent(): 'connect' | 'secureConnect' {
    return this.config.transport === 'TLS' ? 'secureConnect' : 'connect';
  }

  private _waitForConnection(socket: Socket): Promise<this> {
    const ready = this._readyEvent();
    return new Promise<this>((resolve, reject) => {
      const cleanup = () => {
        socket.removeListener(ready, resolveConnect);
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
      socket.once(ready, resolveConnect);
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
    let firstPass = true;
    // Reconnection settings bound the retries, not the endpoints. With them off
    // and several endpoints known - the address the client was configured with,
    // the nodes the roster named - those endpoints were made known in order to
    // be tried, so they get one pass and no backoff, as in the other SDKs. A
    // client that knows one endpoint and turned reconnection off redials
    // nothing, which is what it asked for.
    // Counted rather than tracked within this call: every dial the pass fails
    // closes a socket, and a close starts a reconnect of its own. Bounded by
    // `firstPass` alone, each of those closes would open another sweep and the
    // pass would repeat for as long as the endpoints stay down. The count is
    // reset when a connection is established, so a later loss sweeps again.
    const sweepOnce = !enabled && this._redialCandidates().length > 1;
    while ((enabled && this.reconnectCount < maxRetries) ||
      (sweepOnce && this.reconnectCount < 1)) {
      this.connecting = true;
      this.reconnectCount += 1;
      const candidates = this._redialCandidates();
      // The backoff paces retries against a single endpoint. With other
      // endpoints known there is somewhere else to go, and pausing first only
      // pushes the failover past the interval a caller is willing to wait;
      // later passes still back off.
      if (enabled && (!firstPass || candidates.length === 1))
        await waitForReconnect(interval);
      firstPass = false;
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
      for (const options of candidates) {
        // Re-checked every iteration, not once above the loop: a destroy or a
        // redirect mid-pass has to stop the pass. Left running, the next
        // endpoint that answers would leave an open socket nobody closes, a
        // 'connect' event after the destroy, and the process alive.
        if (this.ending)
          throw new Error('connection is closed', { cause: lastError });
        if (this.socket !== expectedSocket)
          return this.connect();

        const socket = this._installSocket(
          getTransport({ ...this.config, options })
        );
        this.socket = socket;
        expectedSocket = socket;
        try {
          await this._dialWithin(socket, candidates.length > 1);
          if (this.ending) {
            socket.destroy();
            throw new Error('connection is closed', { cause: lastError });
          }
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
  rememberRoster(endpoints: Endpoint[]): void {
    if (endpoints.length === 0)
      return;
    this.rosterEndpoints = endpoints;
  }

  /**
   * The roster endpoint after the one this connection is on, for a request
   * the current node keeps refusing to admit. Metadata and partition
   * consensus groups elect independently, so the metadata leader can hold a
   * follower replica of the partition a request targets, and only walking
   * the roster reaches that group's primary. `undefined` when the roster
   * names nowhere else to go.
   */
  nextRosterEndpoint(visited: Set<string>): Endpoint | undefined {
    const roster = this.rosterEndpoints;
    if (roster.length === 0)
      return undefined;
    const index = roster.findIndex(
      (endpoint) => this.isConnectedTo(endpoint.host, endpoint.port)
    );
    if (index >= 0)
      visited.add(endpointKey(roster[index]));
    const start = index < 0 ? 0 : (index + 1) % roster.length;
    for (let offset = 0; offset < roster.length; offset += 1) {
      const candidate = roster[(start + offset) % roster.length];
      const key = endpointKey(candidate);
      if (visited.has(key) || this.isConnectedTo(candidate.host, candidate.port))
        continue;
      visited.add(key);
      return candidate;
    }
    return undefined;
  }

  /**
   * Endpoints a redial rotates through, likeliest first: where the client
   * currently is, the endpoint it was configured with, then the roster it
   * learned while connected. After a leader redirect the current endpoint may
   * die with the leader, and the rest of the list is the way back to the
   * cluster.
   *
   * Duplicates are dropped by spelling: the loopback aliases and an
   * IPv4-mapped IPv6 address collapse onto one endpoint. Names are not
   * resolved, so a seed given as a DNS name and the roster's IP for the same
   * node still count as two candidates -- one wasted dial per pass, not a
   * correctness problem.
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
        (existing) => existing.port === candidate.port &&
          normalizeHost(existing.host) === normalizeHost(candidate.host)
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
    this.redirecting = true;
    try {
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
    } finally {
      this.redirecting = false;
    }
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

export const endpointKey = (endpoint: Endpoint): string =>
  `${normalizeHost(endpoint.host)}:${endpoint.port}`;
