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
import type { PartitionContext } from '../wire/vsr/header.js';

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
  /** Cancels this call without closing its shared replica link. */
  signal?: AbortSignal,
  /** Whether the response uses the standard command response decoder */
  handleResponse?: boolean,
  /** @deprecated Multiplexed calls ignore this queue-order hint. */
  last?: boolean,
  /** @deprecated The shared session owner manages replica routing. */
  followsLeaderMoves?: boolean,
  /**
   * When the whole request gives up, as an epoch timestamp in milliseconds.
   * Includes admission, discovery, link waits and retries. Defaults to 30 seconds.
   */
  deadline?: number,
  /**
   * Partition context of a poll, an offset store or delete, or a send to an
   * explicit partition, taken from an earlier reply. It replaces the context
   * the client would capture, stays on every retry, and is never refreshed
   * after a history refusal (87): work from one partition incarnation must
   * not land in another.
   */
  context?: PartitionContext,
  /**
   * The `RawClient.lineage` that a call of several commands started with. A
   * command of an older lineage fails with status 40 and sends nothing, so no
   * part of the call runs under a session the caller opened after it started.
   */
  lineage?: number
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
  /** Raised by each sign-in and logout of the caller. */
  readonly lineage?: number
  /** Uses caller credentials; any failed attempt withdraws automatic sign-in. */
  authenticate: (c: ClientCredentials) => Promise<boolean>
  /** Typed login delegates validation to the session owner when supported. */
  _authWithPassword?: (c: PasswordCredentials) => Promise<{ userId: number }>
  _authWithToken?: (c: TokenCredentials) => Promise<{ userId: number }>
  /**
   * Resolves the original frozen operation; never sends it under a fresh identity.
   * The client keeps the proved outcomes of its last `maxPendingOperations`
   * replays, also across a session reset, so a later resolve of the same
   * operation returns the same outcome without a request.
   * `OperationNotRetainedError` means that a session reset or an abandon came
   * before a replay proved the outcome, or that the proved outcome aged out.
   */
  resolveOperation?: (operationId: bigint, options?: SendCommandOptions) => Promise<void>,
  /**
   * Releases a retained lane without asserting that the operation did not commit.
   * A replay that is already running keeps the lane until it ends, and the
   * client keeps the outcome that it proves.
   */
  abandonOperation?: (operationId: bigint) => void,
  /** Destroys the client connection */
  destroy: () => void,
  /** Tracks one facade operation across multiple command submissions. */
  hold?: () => () => void,
  /** Registers an event listener */
  on: (ev: string, cb: (e?: unknown) => void) => void
  /** Registers a one-time event listener */
  once: (ev: string, cb: (e?: unknown) => void) => void
  /** Returns the underlying readable stream */
  getReadStream: () => Readable
}

/**
 * Function type that provides a RawClient instance.
 * Used for dependency injection.
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
   * Initial reconnect delay in milliseconds. Each replica uses exponential
   * backoff capped at 10 seconds with jitter.
   */
  interval: number,
  /**
   * Maximum retries per replica after the first attempt. Bootstrap retries
   * sweep the configured seed and remembered endpoints. The budget limits one
   * reconnect burst: while the client stays signed in, each heartbeat restarts
   * a replica link that used its budget, with a fresh budget.
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
 * Legacy logical-client count. Replica links are managed independently.
 */
export type PoolSizeOption = {
  /** Minimum logical clients; normalized to one */
  min?: number,
  /** Maximum logical clients; only one is supported */
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
  /** Legacy logical-client count; does not limit replica links. */
  poolSize?: PoolSizeOption,
  /** Automatic reconnection configuration */
  reconnect?: ReconnectOption,
  /**
   * Interval for sending heartbeat pings, in milliseconds. Each heartbeat also
   * restarts replica links that used their reconnect budget. 0 disables both.
   */
  heartbeatInterval?: number,
  /** Maximum accepted response frame size in bytes */
  maxResponseFrameSize?: number,
  /** Maximum active plus retained mutations across this client. Default: 256. */
  maxPendingOperations?: number,
  /** Frozen mutation headers and bodies, including retained and writer-owned bytes. Default: 64 MiB. */
  maxPendingOperationBytes?: number
}
