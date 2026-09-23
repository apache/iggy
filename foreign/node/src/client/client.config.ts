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
import type { ClientConfig, ClientConfigOrString } from './client.type.js';
import { parseConnectionString } from './client.connection-string.js';

export const DEFAULT_MAX_RESPONSE_FRAME_SIZE = 64 * 1024 * 1024;

/**
 * The server evicts a connection silent for 36 s (1.2 x its 30 s heartbeat
 * interval). 5 s matches the other SDKs and survives several skipped pings.
 */
export const DEFAULT_HEARTBEAT_INTERVAL = 5 * 1000;

/** Node's largest timer delay: setInterval clamps anything above it to 1 ms. */
export const MAX_HEARTBEAT_INTERVAL = 2_147_483_647;

export const normalizeClientConfig = (
  config: ClientConfigOrString
): ClientConfig => {
  if (typeof config === 'string')
    config = parseConnectionString(config);

  const maxResponseFrameSize =
    config.maxResponseFrameSize ?? DEFAULT_MAX_RESPONSE_FRAME_SIZE;
  if (!Number.isSafeInteger(maxResponseFrameSize) ||
      maxResponseFrameSize < 256)
    throw new TypeError(
      'maxResponseFrameSize must be a safe integer of at least 256 bytes'
    );

  if ((config.poolSize?.min ?? 1) > 1 || (config.poolSize?.max ?? 1) > 1)
    throw new TypeError(
      'VSR clients currently support exactly one pooled connection'
    );

  // Only 0 disables the heartbeat. Anything else unusable has to throw here:
  // the default below is nullish-only, so a value that is negative, fractional
  // or above MAX_HEARTBEAT_INTERVAL reaches setInterval, which clamps it to
  // 1 ms and floods the server.
  const heartbeatInterval =
    config.heartbeatInterval ?? DEFAULT_HEARTBEAT_INTERVAL;
  if (!Number.isSafeInteger(heartbeatInterval) ||
      heartbeatInterval < 0 ||
      heartbeatInterval > MAX_HEARTBEAT_INTERVAL)
    throw new TypeError(
      `heartbeatInterval must be a safe integer of milliseconds between 0 and ${MAX_HEARTBEAT_INTERVAL} (0 disables heartbeats)`
    );

  // Unlike the heartbeat, 0 is not a disable here: an immediate retry delay
  // turns reconnection into a hot loop. The ceiling guards the same
  // setInterval clamp, which would turn a long backoff into a 1 ms spin.
  // A disabled reconnect never schedules a retry, so the interval check
  // applies only when enabled; callers commonly pass a zero interval with
  // enabled: false. maxRetries stays bounded either way, matching the
  // connection-string path.
  if (config.reconnect !== undefined) {
    const { enabled, interval, maxRetries } = config.reconnect;
    if (!Number.isSafeInteger(maxRetries) ||
        maxRetries < 0 ||
        maxRetries > MAX_U32)
      throw new TypeError(
        `reconnect.maxRetries must be a non-negative integer of at most ${MAX_U32}`
      );
    if (enabled &&
        (!Number.isSafeInteger(interval) ||
          interval < 1 ||
          interval > MAX_HEARTBEAT_INTERVAL))
      throw new TypeError(
        `reconnect.interval must be a safe integer of milliseconds between 1 and ${MAX_HEARTBEAT_INTERVAL}`
      );
  }

  return {
    ...config,
    options: { ...config.options },
    maxResponseFrameSize,
    heartbeatInterval,
    poolSize: { min: 1, max: 1 }
  };
};
