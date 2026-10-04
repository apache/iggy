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

import { MAX_U32 } from '../constant.js';
import {
  nanosecondsToMilliseconds,
  parseIggyDurationNanoseconds
} from '../duration.utils.js';
import type { ClientConfig, ReconnectOption } from './client.type.js';

const DEFAULT_PROTOCOL = 'iggy';
const SCHEME_PREFIX = 'iggy+';
const SUPPORTED_PROTOCOLS = ['tcp'] as const;

/** Reconnection defaults carried by every connection string.
 *
 * Mirrors TcpConnectionStringOptions: retries default to unlimited and the
 * interval to 1s. Deliberately not DefaultReconnectOption, whose 12/5s pair
 * only applies to object configs.
 */
const CONNECTION_STRING_RECONNECT: ReconnectOption = {
  enabled: true,
  interval: 1000,
  maxRetries: MAX_U32
};

/** Parses a duration with the exact grammar of the Rust SDK's
 * `IggyDuration::from_str`, returning whole milliseconds.
 */
export const parseDuration = (value: string): number => {
  try {
    return nanosecondsToMilliseconds(parseIggyDurationNanoseconds(value));
  } catch (error) {
    throw new TypeError(`invalid duration "${value}"`, { cause: error });
  }
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
    throw new TypeError('invalid connection string');

  const scheme = protocolParts[0];
  const protocol = scheme === DEFAULT_PROTOCOL
    ? 'tcp'
    : scheme.startsWith(SCHEME_PREFIX)
      ? scheme.slice(SCHEME_PREFIX.length)
      : undefined;
  if (protocol === undefined)
    throw new TypeError('invalid connection string');
  if (!SUPPORTED_PROTOCOLS.includes(protocol as (typeof SUPPORTED_PROTOCOLS)[number]))
    throw new TypeError(
      `unsupported transport "${protocol}", Node SDK supports tcp only`
    );

  const parts = protocolParts[1].split('@');
  if (parts.length !== 2)
    throw new TypeError('invalid connection string');

  const credentials = parts[0].split(':');
  const tokenCredentials = credentials.length === 1;
  if (!tokenCredentials && credentials.length !== 2)
    throw new TypeError('invalid connection string');

  const username = credentials[0];
  const password = credentials[1] ?? '';
  if (!tokenCredentials && (username.length === 0 || password.length === 0))
    throw new TypeError('invalid connection string');

  const serverAndOptions = parts[1].split('?');
  if (serverAndOptions.length > 2)
    throw new TypeError('invalid connection string');

  const serverAddress = serverAndOptions[0];
  // One match covers both `[ipv6]:port` and `host:port`, where the
  // unbracketed host may not contain a colon. Multi-colon authorities such
  // as `2001:db8::1:8090` or `host:8090:9090` are rejected
  const addressMatch = /^(?:\[([^\]]+)\]|([^:\[\]]+)):(\d+)$/.exec(serverAddress);
  if (!addressMatch)
    throw new TypeError('invalid connection string');

  const host = addressMatch[1] ?? addressMatch[2];
  const port = Number(addressMatch[3]);
  if (port > 65535)
    throw new TypeError('invalid connection string');

  const options: ParsedConnectionOptions = serverAndOptions.length === 2
    ? parseConnectionOptions(serverAndOptions[1])
    : {
        tls: false,
        reconnect: { ...CONNECTION_STRING_RECONNECT }
      };
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
      : { username, password },
    // Always present on connection strings: Rust applies its unlimited/1s
    // reconnection defaults even without query options.
    reconnect
  };
  if (heartbeatInterval !== undefined)
    config.heartbeatInterval = heartbeatInterval;

  return config;
};

type ParsedConnectionOptions = {
  tls: boolean,
  noDelay?: boolean,
  servername?: string,
  /** Path stored for connect-time reading, match Rust SDK. */
  caFile?: string,
  reconnect: ReconnectOption,
  heartbeatInterval?: number
};

const parseConnectionOptions = (
  optionsString: string
): ParsedConnectionOptions => {
  const parsed: ParsedConnectionOptions = {
    tls: false,
    reconnect: { ...CONNECTION_STRING_RECONNECT }
  };
  for (const option of optionsString.split('&')) {
    const optionParts = option.split('=');
    if (optionParts.length !== 2)
      throw new TypeError('invalid connection string');
    const [name, value] = optionParts;
    switch (name) {
      case 'tls':
        parsed.tls = parseBoolean(name, value);
        break;
      case 'nodelay':
        parsed.noDelay = parseBoolean(name, value);
        break;
      case 'tls_domain':
        parsed.servername = value;
        break;
      case 'tls_ca_file':
        // The path is stored unread; the certificate is loaded when the
        // TLS socket is created, so plain-TCP configs never touch the
        // filesystem (matches the Rust SDK).
        parsed.caFile = value;
        break;

      case 'reconnection_retries': {
        // Values above u32::MAX are rejected like the Rust SDK's u32
        // overflow; otherwise they would act as a second, undocumented
        // spelling of "unlimited".
        const maxRetries = value === 'unlimited'
          ? MAX_U32
          : parseNumber(name, value);
        if (maxRetries > MAX_U32)
          throw new TypeError(
            `option "${name}" must be at most ${MAX_U32}`
          );
        parsed.reconnect.maxRetries = maxRetries;
        break;
      }
      case 'reconnection_interval': {
        const interval = parseDuration(value);
        // With retries defaulting to unlimited, a zero interval would turn
        // the reconnect loop into an unbounded hot loop.
        if (interval <= 0)
          throw new TypeError(`option "${name}" must be positive`);
        parsed.reconnect.interval = interval;
        break;
      }
      case 'reestablish_after':
        // No Node equivalent: validated as a duration like the Rust SDK
        // does, then discarded.
        parseDuration(value);
        break;
      case 'heartbeat_interval':
        parsed.heartbeatInterval = parseDuration(value);
        break;
      default:
        throw new TypeError(`unknown option "${name}"`);
    }
  }
  return parsed;
};

const parseBoolean = (name: string, value: string): boolean => {
  if (value !== 'true' && value !== 'false')
    throw new TypeError(`option "${name}" must be true or false`);
  return value === 'true';
};

const parseNumber = (name: string, value: string): number => {
  if (!/^\d+$/.test(value))
    throw new TypeError(`option "${name}" must be a non-negative integer`);
  return Number(value);
};
