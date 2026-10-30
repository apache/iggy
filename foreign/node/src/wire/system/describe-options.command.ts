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

import type { CommandResponse } from '../../client/index.js';
import { COMMAND_CODE } from '../command.code.js';
import { wrapCommand } from '../command.utils.js';

/** Resource whose option catalog the server serves. */
export const OptionsScope = {
  Topic: 1,
  Stream: 2,
  User: 3
} as const;

export type OptionsScope = typeof OptionsScope[keyof typeof OptionsScope];

/** Bits of the `flags` byte a `DescribeOptions` entry carries. */
export const OptionFlag = {
  Updatable: 1,
  Resettable: 2,
  Secret: 4,
  ReadOnly: 8,
  Critical: 16
} as const;

/**
 * One catalog entry: the key, the kind the server encodes its default under,
 * what a client may do with it, that default, and what the option does.
 *
 * `kind` is this key's canonical kind: what the server encodes its default
 * under, and what a value set at create or update is stored as whatever kind it
 * was sent in, since admission re-encodes every block from its own parse.
 */
export type OptionSpec = {
  key: string,
  kind: number,
  /** An update may set the key */
  updatable: boolean,
  /** An update may reset the key to its default */
  resettable: boolean,
  /** Write-only: accepted at create, never returned */
  secret: boolean,
  /** Computed by the server and returned on reads, never accepted */
  readOnly: boolean,
  /** A node that does not understand the key must not serve the resource */
  critical: boolean,
  defaultValue: Buffer,
  description: string
};

export type DescribeOptions = {
  scope: OptionsScope
};

const deserializeDescribeOptions = (b: Buffer): OptionSpec[] => {
  const count = b.readUInt32LE(0);
  let position = 4;
  const specs: OptionSpec[] = [];
  for (let i = 0; i < count; i++) {
    const keyLength = b.readUInt8(position);
    position += 1;
    if (position + keyLength > b.length)
      throw new Error(`Option key overruns the response at offset ${position}`);
    const key = b.subarray(position, position + keyLength).toString();
    position += keyLength;

    const kind = b.readUInt8(position);
    const flags = b.readUInt8(position + 1);
    position += 2;

    const defaultLength = b.readUInt32LE(position);
    position += 4;
    if (position + defaultLength > b.length)
      throw new Error(`Default value for '${key}' overruns the response`);
    const defaultValue = Buffer.from(
      b.subarray(position, position + defaultLength)
    );
    position += defaultLength;

    const descriptionLength = b.readUInt32LE(position);
    position += 4;
    if (position + descriptionLength > b.length)
      throw new Error(`Description for '${key}' overruns the response`);
    const description = b.subarray(
      position, position + descriptionLength
    ).toString();
    position += descriptionLength;

    specs.push({
      key,
      kind,
      updatable: (flags & OptionFlag.Updatable) !== 0,
      resettable: (flags & OptionFlag.Resettable) !== 0,
      secret: (flags & OptionFlag.Secret) !== 0,
      readOnly: (flags & OptionFlag.ReadOnly) !== 0,
      critical: (flags & OptionFlag.Critical) !== 0,
      defaultValue,
      description
    });
  }
  return specs;
};

export const DESCRIBE_OPTIONS = {
  code: COMMAND_CODE.DescribeOptions,

  serialize: ({ scope }: DescribeOptions) => {
    return Buffer.from([scope]);
  },

  deserialize: (r: CommandResponse): OptionSpec[] =>
    deserializeDescribeOptions(r.data)
};

export const describeOptions = wrapCommand<DescribeOptions, OptionSpec[]>(
  DESCRIBE_OPTIONS
);
