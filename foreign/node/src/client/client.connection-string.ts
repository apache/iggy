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

import { readFileSync } from 'node:fs';
import type { ClientConfig, ReconnectOption } from './client.type.js';

const DEFAULT_PROTOCOL = 'iggy';
const SCHEME_PREFIX = 'iggy+';
const SUPPORTED_PROTOCOLS = ['tcp'] as const;

/** Duration units in milliseconds. */
const DURATION_UNITS = {
  ms: 1,
  s: 1000,
  m: 60 * 1000,
  h: 60 * 60 * 1000
} as const;

/** Parses a duration such as "500ms" or "5s" into milliseconds. */
export const parseDuration = (value: string): number => {
  const match = /^(\d+(?:\.\d+)?)(ms|s|m|h)$/.exec(value);
  if (!match)
    throw new TypeError(`invalid duration in connection string: "${value}"`);
  return Number(match[1]) * DURATION_UNITS[match[2] as keyof typeof DURATION_UNITS];
};

/**
 * Parses an Iggy connection string into a client configuration.
 *
 * Supports `iggy://` and `iggy+tcp://`; the Node SDK implements TCP/TLS only.
 * Credentials are either `username:password` or a single personal access
 * token before the `@`. TLS is enabled with `tls=true`.
 */
export const parseConnectionString = (connectionString: string): ClientConfig => {
  if (typeof connectionString !== 'string' || connectionString.length === 0)
    throw new TypeError('connection string must be a non-empty string');

  const protocolParts = connectionString.split('://');
  if (protocolParts.length !== 2)
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const scheme = protocolParts[0];
  const protocol = scheme === DEFAULT_PROTOCOL
    ? 'tcp'
    : scheme.startsWith(SCHEME_PREFIX)
      ? scheme.slice(SCHEME_PREFIX.length)
      : undefined;
  if (protocol === undefined)
    throw new TypeError(`invalid connection string: "${connectionString}"`);
  if (!SUPPORTED_PROTOCOLS.includes(protocol as (typeof SUPPORTED_PROTOCOLS)[number]))
    throw new TypeError(
      `unsupported transport "${protocol}" in connection string, ` +
      'Node SDK supports tcp only'
    );

  const parts = protocolParts[1].split('@');
  if (parts.length !== 2)
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const credentials = parts[0].split(':');
  const tokenCredentials = credentials.length === 1;
  if (!tokenCredentials && credentials.length !== 2)
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const username = credentials[0];
  const password = credentials[1] ?? '';
  if (!tokenCredentials && (username.length === 0 || password.length === 0))
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const serverAndOptions = parts[1].split('?');
  if (serverAndOptions.length > 2)
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const serverAddress = serverAndOptions[0];
  if (serverAddress.length === 0 ||
      !serverAddress.includes(':') ||
      serverAddress.startsWith(':'))
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const port = serverAddress.slice(serverAddress.lastIndexOf(':') + 1);
  if (port.length === 0 || !/^\d+$/.test(port) || Number(port) > 65535)
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const host = serverAddress.slice(0, serverAddress.lastIndexOf(':'));
  if (host.length === 0)
    throw new TypeError(`invalid connection string: "${connectionString}"`);

  const options: ParsedConnectionOptions = serverAndOptions[1]
    ? parseConnectionOptions(serverAndOptions[1], connectionString)
    : { tls: false };
  const { tls, reconnect, heartbeatInterval, ...transportOptions } = options;

  const config: ClientConfig = {
    transport: tls ? 'TLS' : 'TCP',
    options: {
      host,
      port: Number(port),
      ...transportOptions
    },
    credentials: tokenCredentials
      ? { token: username }
      : { username, password }
  };
  if (reconnect)
    config.reconnect = reconnect;
  if (heartbeatInterval !== undefined)
    config.heartbeatInterval = heartbeatInterval;

  return config;
};

type ParsedConnectionOptions = {
  tls: boolean,
  noDelay?: boolean,
  servername?: string,
  ca?: Buffer,
  reconnect?: ReconnectOption,
  heartbeatInterval?: number
};

const parseConnectionOptions = (
  optionsString: string,
  connectionString: string
): ParsedConnectionOptions => {
  const parsed: ParsedConnectionOptions = { tls: false };
  for (const option of optionsString.split('&')) {
    const optionParts = option.split('=');
    if (optionParts.length !== 2)
      throw new TypeError(`invalid connection string: "${connectionString}"`);
    const [name, value] = optionParts;
    switch (name) {
      case 'tls':
        parsed.tls = parseBoolean(name, value, connectionString);
        break;
      case 'nodelay':
        parsed.noDelay = parseBoolean(name, value, connectionString);
        break;
      case 'tls_domain':
        parsed.servername = value;
        break;
      case 'tls_ca_file':
        parsed.ca = readFileSync(value);
        break;
      case 'reconnection_retries':
        parsed.reconnect = {
          enabled: true,
          interval: parsed.reconnect?.interval ?? 5000,
          maxRetries: value === 'unlimited'
            ? Number.MAX_SAFE_INTEGER
            : parseNumber(name, value, connectionString)
        };
        break;
      case 'reconnection_interval':
        parsed.reconnect = {
          enabled: true,
          maxRetries: parsed.reconnect?.maxRetries ?? 12,
          interval: parseDuration(value)
        };
        break;
      case 'reestablish_after':
        // No Node equivalent: accepted for format compatibility.
        break;
      case 'heartbeat_interval':
        parsed.heartbeatInterval = parseDuration(value);
        break;
      default:
        throw new TypeError(
          `unknown option "${name}" in connection string: "${connectionString}"`
        );
    }
  }
  return parsed;
};

const parseBoolean = (
  name: string,
  value: string,
  connectionString: string
): boolean => {
  if (value !== 'true' && value !== 'false')
    throw new TypeError(
      `option "${name}" must be true or false in connection string: "${connectionString}"`
    );
  return value === 'true';
};

const parseNumber = (
  name: string,
  value: string,
  connectionString: string
): number => {
  if (!/^\d+$/.test(value))
    throw new TypeError(
      `option "${name}" must be a non-negative integer in connection string: "${connectionString}"`
    );
  return Number(value);
};
