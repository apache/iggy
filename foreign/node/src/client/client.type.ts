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


import type { Readable } from 'stream';
import { type TcpSocketConnectOpts } from 'node:net';
import { type ConnectionOptions } from 'node:tls';

/**
 * TCP socket connection options.
 * Alias for Node.js TcpSocketConnectOpts.
 */
export type TcpOption = TcpSocketConnectOpts;

/**
 * TLS socket connection options.
 * Combines port number with Node.js TLS ConnectionOptions.
 */
export type TlsOption = { port: number } & ConnectionOptions;

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

/**
 * Low-level client interface for communicating with the Iggy server.
 * Provides direct access to command sending and event handling.
 */
export type RawClient = {
  /** Sends a command to the server and returns the response */
  sendCommand: (
    code: number, payload: Buffer, handleResponse?: boolean
  ) => Promise<CommandResponse>,
  /** Whether the client has been authenticated */
  isAuthenticated: boolean
  /** Authenticates the client with the server */
  authenticate: (c: ClientCredentials) => Promise<boolean>
  /** Destroys the client connection */
  destroy: () => void,
  /** Registers an event listener */
  on: (ev: string, cb: (e?: unknown) => void) => void
  /** Registers a one-time event listener */
  once: (ev: string, cb: (e?: unknown) => void) => void
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
  /** Interval between reconnection attempts in milliseconds */
  interval: number,
  /** Maximum number of reconnection attempts */
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
  /** Interval for sending heartbeat pings in milliseconds */
  heartbeatInterval?: number
}
