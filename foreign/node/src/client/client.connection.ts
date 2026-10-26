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
import type { ClientConfig, TlsOption, TcpOption } from "./client.type.js"
import { debug } from './client.debug.js';
import { DEFAULT_MAX_RESPONSE_FRAME_SIZE } from './client.config.js';
import {
  ProtocolFrameError,
  ResponseFrameDecoder
} from './client.frame.js';
import {
  Command, EvictionReason, peekCommand, readEviction, readStatus, REQUEST_OFFSET, REPLY_OFFSET
} from '../wire/vsr/header.js';
import {
  ChangeSignal, PermitPool, type FrozenFrame, OPERATION_TIMEOUT_MS, VsrResponseTimeoutError, waitWithin
} from './client.operation.js';
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

export type Endpoint = { host: string, port: number };
export const DIAL_TIMEOUT_MS = 2_000;
export const LINK_WRITE_QUEUE = 32;
export const MAX_PENDING_PER_LINK = 256;
const STALE_CLIENT = 30;

/** A connected socket was lost; a later bootstrap can bind it again. */
export class ConnectionClosedError extends Error {
  constructor(cause?: Error) {
    super(cause?.message ?? 'replica connection closed', { cause });
    this.name = 'ConnectionClosedError';
  }
}

export type LinkAttempt = { response?: Buffer, error?: unknown, started: boolean };
type Pending = {
  readonly id: bigint,
  readonly frame: Buffer,
  readonly owner?: FrozenFrame,
  readonly resolve: (result: LinkAttempt) => void,
  done: boolean,
  started: boolean
};

/** One physical endpoint, with independent reader framing and a bounded writer. */
export class IggyConnection extends EventEmitter {
  socket!: Socket;
  connected = false;
  ending = false;
  failure?: unknown;
  readonly closed: Promise<void>;
  private close!: () => void;
  private connectPromise?: Promise<this>;
  private readonly decoder: ResponseFrameDecoder;
  private readonly pending = new Map<bigint, Pending>();
  private readonly writes: Pending[] = [];
  private readonly changed = new ChangeSignal();
  private readonly admissions = new PermitPool(MAX_PENDING_PER_LINK);
  private writing = false;
  private frameTimer?: NodeJS.Timeout;
  private failed = false;

  constructor(public readonly config: ClientConfig) {
    super();
    this.decoder = new ResponseFrameDecoder(config.maxResponseFrameSize ?? DEFAULT_MAX_RESPONSE_FRAME_SIZE);
    this.closed = new Promise<void>((resolve) => { this.close = resolve; });
  }

  connect(signal?: AbortSignal): Promise<this> {
    if (this.ending || this.failed)
      return Promise.reject(this.failure ?? new Error('connection is closed'));
    if (this.connected)
      return Promise.resolve(this);
    if (this.connectPromise)
      return this.connectPromise;
    this.connectPromise = this.dial(signal);
    return this.connectPromise;
  }

  private async dial(signal?: AbortSignal): Promise<this> {
    try {
      signal?.throwIfAborted();
      const socket = getTransport(this.config);
      this.socket = socket;
      const ready = this.config.transport === 'TLS' ? 'secureConnect' : 'connect';
      socket.on('data', (data) => this.onData(data));
      socket.on('error', (error) => this.fail(this.connected ? new ConnectionClosedError(error) : error));
      socket.once('close', () => this.fail(this.failure ?? new ConnectionClosedError()));
      await waitWithin(new Promise<void>((resolve, reject) => {
        const cleanup = () => {
          socket.removeListener(ready, established);
          socket.removeListener('error', rejected);
          socket.removeListener('close', closed);
        };
        const established = () => { cleanup(); resolve(); };
        const rejected = (error: Error) => { cleanup(); reject(error); };
        const closed = () => rejected(new Error('connection closed before establishment'));
        socket.once(ready, established);
        socket.once('error', rejected);
        socket.once('close', closed);
      }), Date.now() + DIAL_TIMEOUT_MS, signal);
      if (this.failed || this.ending)
        throw this.failure ?? new Error('connection is closed');
      this.connected = true;
      this.emit('connect');
      return this;
    } catch (error) {
      this.fail(error);
      throw error;
    }
  }

  async exchange(frame: () => Buffer, deadline: number, signal: AbortSignal, owner?: FrozenFrame): Promise<LinkAttempt> {
    let release: (() => void) | undefined;
    let pending: Pending | undefined;
    try {
      release = await this.admissions.acquire(deadline, signal);
      signal.throwIfAborted();
      if (Date.now() >= deadline)
        throw new VsrResponseTimeoutError(0);
      if (!this.connected || this.failed)
        throw this.failure ?? new Error('replica connection is not ready');
      const bytes = frame();
      const id = bytes.readBigUInt64LE(REQUEST_OFFSET.request);
      if (this.pending.has(id))
        throw new Error('request id already has an active exchange');
      let resolve!: (attempt: LinkAttempt) => void;
      const response = new Promise<LinkAttempt>((complete) => { resolve = complete; });
      pending = { id, frame: bytes, owner, resolve, done: false, started: false };
      this.pending.set(id, pending);
      while (this.writes.length >= LINK_WRITE_QUEUE && !pending.done) {
        await waitWithin(this.changed.promise, deadline, signal);
      }
      signal.throwIfAborted();
      if (!pending.done) {
        owner?.retain();
        this.writes.push(pending);
        void this.writeFrames();
      }
      return await waitWithin(response, deadline, signal);
    } catch (error) {
      if (pending)
        this.finish(pending, { error, started: pending.started });
      return { error, started: pending?.started ?? false };
    } finally {
      release?.();
    }
  }

  private async writeFrames(): Promise<void> {
    if (this.writing)
      return;
    this.writing = true;
    try {
      while (this.writes.length > 0) {
        const pending = this.writes.shift()!;
        this.changed.pulse();
        try {
          if (pending.done)
            continue;
          pending.started = true;
          const written = new Promise<void>((resolve, reject) => {
            this.socket.write(pending.frame, (error?: Error | null) => error ? reject(new ConnectionClosedError(error)) : resolve());
          });
          try {
            await waitWithin(written, Date.now() + OPERATION_TIMEOUT_MS);
          } catch (error) {
            this.fail(error);
            await written.catch(() => undefined);
          }
        } finally {
          pending.owner?.release();
        }
      }
    } catch (error) {
      this.fail(error);
    } finally {
      this.writing = false;
    }
  }

  private onData(data: Buffer | string): void {
    try {
      if (!Buffer.isBuffer(data))
        throw new ProtocolFrameError('socket returned text instead of binary data');
      const frames = this.decoder.push(data);
      if (!this.decoder.hasBufferedData) {
        clearTimeout(this.frameTimer);
        this.frameTimer = undefined;
      } else if (!this.frameTimer || frames.length > 0) {
        clearTimeout(this.frameTimer);
        this.frameTimer = setTimeout(() => this.fail(new ProtocolFrameError('incomplete consensus frame timed out')),
          OPERATION_TIMEOUT_MS);
      }
      for (const response of frames) {
        if (peekCommand(response) === Command.Eviction) {
          const error = evictionError(response);
          this.emit('eviction', error);
          // The server keeps the session of a stale client and drops only this
          // link, so it fails as a lost link and the client rebinds the session.
          this.fail(readEviction(response).reason === EvictionReason.StaleClient
            ? new Error('replica link evicted as a stale client', { cause: error }) : error);
          return;
        }
        if (peekCommand(response) !== Command.Reply)
          throw new ProtocolFrameError('unexpected consensus frame');
        const id = response.readBigUInt64LE(REPLY_OFFSET.request);
        const pending = this.pending.get(id);
        if (pending)
          this.finish(pending, { response, started: pending.started });
        // A 30 refusal says that the server no longer attaches this link to the
        // session. A replica that replaced its client table says so of a live
        // session too, so the link fails as a lost link and the bind decides.
        if (readStatus(response) === STALE_CLIENT) {
          this.fail(new Error('replica link detached from its session'));
          return;
        }
      }
    } catch (error) {
      this.fail(error);
    }
  }

  private finish(pending: Pending, result: LinkAttempt): void {
    if (pending.done)
      return;
    pending.done = true;
    if (this.pending.get(pending.id) === pending)
      this.pending.delete(pending.id);
    pending.resolve(result);
    this.changed.pulse();
  }

  private fail(error: unknown): void {
    if (this.failed)
      return;
    this.failed = true;
    this.failure = error;
    this.connected = false;
    clearTimeout(this.frameTimer);
    this.decoder.clear();
    for (const pending of this.pending.values())
      this.finish(pending, { error, started: pending.started });
    for (const pending of this.writes.splice(0))
      pending.owner?.release();
    this.socket?.destroy();
    this.close();
    this.changed.pulse();
    if (this.listenerCount('error') > 0)
      this.emit('error', error);
    this.emit('disconnected');
    debug('replica link closed', error);
  }

  isConnectedTo(host: string, port: number): boolean {
    const target = normalizeHost(host);
    if (this.socket?.remotePort === port && normalizeHost(this.socket.remoteAddress) === target)
      return true;
    return this.config.options.port === port && normalizeHost(this.config.options.host) === target;
  }

  _destroy(): void {
    this.ending = true;
    this.fail(new Error('connection destroyed'));
  }
}

const normalizeHost = (host?: string): string => {
  const normalized = (host ?? '').toLowerCase().replace(/^::ffff:/, '');
  return normalized === 'localhost' || normalized === '::1' ? '127.0.0.1' : normalized;
};
export const endpointKey = (endpoint: Endpoint): string => `${normalizeHost(endpoint.host)}:${endpoint.port}`;
