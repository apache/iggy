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

export type TcpOption = TcpSocketConnectOpts;
export type TlsOption = { port: number } & ConnectionOptions;

export type CommandResponse = {
  status: number,
  length: number,
  data: Buffer
};

export type RawClient = {
  sendCommand: (
    code: number, payload: Buffer, handleResponse?: boolean
  ) => Promise<CommandResponse>,
  isAuthenticated: boolean
  authenticate: (c: ClientCredentials) => Promise<boolean>
  destroy: () => void,
  on: (ev: string, cb: (e?: unknown) => void) => void
  once: (ev: string, cb: (e?: unknown) => void) => void
  getReadStream: () => Readable
}

export type ClientProvider = () => Promise<RawClient>;

export const Transports = ['TCP', 'TLS' /**, 'QUIC' */] as const;
export type TransportType = typeof Transports[number];

export type ReconnectOption = {
  enabled: boolean,
  interval: number,
  maxRetries: number
}

export type TransportOption = TcpOption | TlsOption;

export type TokenCredentials = {
  token: string
}

export type PasswordCredentials = {
  username: string,
  password: string
}

export type ClientCredentials = TokenCredentials | PasswordCredentials;

export type PoolSizeOption = {
  min?: number,
  max?: number
}

export type ClientConfig = {
  transport: TransportType,
  options: TransportOption,
  credentials: ClientCredentials,
  poolSize?: PoolSizeOption,
  reconnect?: ReconnectOption,
  heartbeatInterval?: number
}
