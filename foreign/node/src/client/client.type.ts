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

import type { Readable } from 'stream';
import { type TcpNetConnectOpts } from 'node:net';
import { type ConnectionOptions } from 'node:tls';

/**
 * TCP socket connection options.
 * Alias for Node.js TcpNetConnectOpts, what net.createConnection accepts.
 */
export type TcpOption = TcpNetConnectOpts;

/**
 * TLS socket connection options.
 * Combines port number with Node.js TLS ConnectionOptions and the
 * net.connect options tls.connect forwards at runtime. `caFile` is an SDK
 * extension: a CA certificate path read when the TLS socket is created.
 */
export type TlsOption = { port: number } & ConnectionOptions & Partial<TcpNetConnectOpts> & { caFile?: string };

/**
 * Response from a command sent to the Iggy server.
 */
export type CommandResponse = {
  /** Response status code (0 indicates success) */
  status: number,
  /** Length of the response data in bytes */
  length: number,
  /** Response payload data */
  data: Buffer
};

export type SendCommandOptions = {
  /** Whether the response uses the standard command response decoder */
  handleResponse?: boolean,
  /** Whether to append rather than prepend the command to the queue */
  last?: boolean,
  /**
   * Whether a not-admitted refusal re-checks the leader and re-issues the
   * command. False for the roster read a re-check itself runs: answering a
   * leader check with another leader check would recurse.
   */
  followsLeaderMoves?: boolean,
  /**
   * When the whole request gives up, as an epoch timestamp in milliseconds.
   * Set when a command already carries a budget -- one re-issued after a
   * leader move keeps the budget it was first submitted with, rather than
   * opening a second one on top of it. Defaults to a fresh response timeout.
   */
  deadline?: number
};

export type ClientEventMap = {
  /** Emitted when a transport connection attempt starts */
  connecting: [],
  /** Emitted when the TCP connection or TLS handshake is ready */
  connected: [],
  /** Emitted when the transport reports an error */
  error: [error: Error]
};

export type RawClientEventMap = ClientEventMap & {
  eviction: [error: Error],
  finishQueue: [],
  heartbeat: [],
  sessionReset: []
};

/**
 * Low-level client interface for communicating with the Iggy server.
 * Provides direct access to command sending and event handling.
 */
export type RawClient = {
  /** Sends a command to the server and returns the response */
  sendCommand: (
    code: number,
    payload: Buffer,
    options?: SendCommandOptions
  ) => Promise<CommandResponse>,
  /** Whether the client has been authenticated */
  isAuthenticated: boolean
  /** Authenticates the client with the server */
  authenticate: (c: ClientCredentials) => Promise<boolean>
  /** Destroys the client connection */
  destroy: () => void,
  /** Holds a pooled client across multiple command submissions */
  hold?: () => () => void,
  /** Registers an event listener */
  on: <Event extends keyof RawClientEventMap>(
    event: Event,
    listener: (...args: RawClientEventMap[Event]) => void
  ) => void
  /** Registers a one-time event listener */
  once: <Event extends keyof RawClientEventMap>(
    event: Event,
    listener: (...args: RawClientEventMap[Event]) => void
  ) => void
  /** Returns the underlying readable stream */
  getReadStream: () => Readable
}

/**
 * Function type that provides a RawClient instance.
 * Used for dependency injection and connection pooling.
 */
export type ClientProvider = () => Promise<RawClient>;

/**
 * Available transport protocols for connecting to the Iggy server.
 */
export const Transports = ['TCP', 'TLS' /**, 'QUIC' */] as const;

/**
 * Transport protocol type.
 * Currently supports 'TCP' and 'TLS'.
 */
export type TransportType = typeof Transports[number];

/**
 * Configuration options for automatic reconnection.
 */
export type ReconnectOption = {
  /** Whether automatic reconnection is enabled */
  enabled: boolean,
  /**
   * Milliseconds to wait between passes. The first pass runs at once when more
   * than one endpoint is known.
   */
  interval: number,
  /**
   * Maximum number of passes over the known endpoints. One pass dials the
   * endpoint the client is on, the endpoint it was configured with, and every
   * node the roster named, so this counts passes rather than dials.
   */
  maxRetries: number
}

/**
 * Union type for transport-specific connection options.
 */
export type TransportOption = TcpOption | TlsOption;

/**
 * Token-based authentication credentials.
 */
export type TokenCredentials = {
  /** Authentication token */
  token: string
}

/**
 * Username/password authentication credentials.
 */
export type PasswordCredentials = {
  /** Username for authentication */
  username: string,
  /** Password for authentication */
  password: string
}

/**
 * Union type for client authentication credentials.
 * Supports either token-based or password-based authentication.
 */
export type ClientCredentials = TokenCredentials | PasswordCredentials;

/**
 * Connection pool size configuration.
 */
export type PoolSizeOption = {
  /** Minimum number of connections in the pool */
  min?: number,
  /** Maximum number of connections in the pool */
  max?: number
}

/**
 * Client configuration or a connection string such as
 * `iggy://username:password@host:port`.
 */
export type ClientConfigOrString = ClientConfig | string;

/**
 * Complete client configuration for connecting to the Iggy server.
 */
export type ClientConfig = {
  /** Transport protocol to use (TCP or TLS) */
  transport: TransportType,
  /** Transport-specific connection options */
  options: TransportOption,
  /** Authentication credentials */
  credentials: ClientCredentials,
  /** Connection pool size configuration */
  poolSize?: PoolSizeOption,
  /** Automatic reconnection configuration */
  reconnect?: ReconnectOption,
  /** Interval for sending heartbeat pings, in milliseconds */
  heartbeatInterval?: number,
  /** Maximum accepted response frame size in bytes */
  maxResponseFrameSize?: number
}
