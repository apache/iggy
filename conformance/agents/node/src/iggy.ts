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

// The SDK of this repository, built in foreign/node with `npm ci && npm run build`.
// The agent imports its build output, so it always runs the SDK of its own commit.

import { readFileSync } from 'node:fs';
import type { Client, PolledBatch } from '../../../../foreign/node/dist/index.js';

export * from '../../../../foreign/node/dist/index.js';

const sdkPackage = new URL('../../../../foreign/node/package.json', import.meta.url);

/** The version of the SDK package that the agent runs. */
export const SDK_VERSION: string = (JSON.parse(readFileSync(sdkPackage, 'utf8')) as { version: string }).version;

// The SDK exports these result types under no name of their own.
export type Stream = NonNullable<Awaited<ReturnType<Client['stream']['get']>>>;
export type Topic = NonNullable<Awaited<ReturnType<Client['topic']['get']>>>;
export type User = NonNullable<Awaited<ReturnType<Client['user']['get']>>>;
export type UserPermissions = NonNullable<User['permissions']>;
export type ConsumerGroup = NonNullable<Awaited<ReturnType<Client['group']['get']>>>;
export type Message = PolledBatch['messages'][number];
export type CreateMessage = Parameters<Client['message']['send']>[0]['messages'][number];
