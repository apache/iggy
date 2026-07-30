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

import type { ClientConfig, Protocol } from './client.type.js';

export const DEFAULT_MAX_RESPONSE_FRAME_SIZE = 64 * 1024 * 1024;

const isProtocol = (value: unknown): value is Protocol =>
  value === 'classic' || value === 'vsr';

export const normalizeClientConfig = (
  config: ClientConfig
): ClientConfig & { protocol: Protocol } => {
  const protocol = config.protocol ?? 'classic';
  if (!isProtocol(protocol))
    throw new TypeError(`unsupported wire protocol: ${String(protocol)}`);

  const maxResponseFrameSize =
    config.maxResponseFrameSize ?? DEFAULT_MAX_RESPONSE_FRAME_SIZE;
  if (!Number.isSafeInteger(maxResponseFrameSize) ||
      maxResponseFrameSize < 256)
    throw new TypeError(
      'maxResponseFrameSize must be a safe integer of at least 256 bytes'
    );

  if (protocol === 'vsr' && config.transport === 'TLS')
    throw new TypeError('VSR framing currently supports the TCP transport only');

  if (protocol === 'vsr' &&
      ((config.poolSize?.min ?? 1) > 1 || (config.poolSize?.max ?? 1) > 1))
    throw new TypeError(
      'VSR clients currently support exactly one pooled connection'
    );

  return {
    ...config,
    protocol,
    options: { ...config.options },
    maxResponseFrameSize,
    ...(protocol === 'vsr' ? { poolSize: { min: 1, max: 1 } } : {})
  };
};
