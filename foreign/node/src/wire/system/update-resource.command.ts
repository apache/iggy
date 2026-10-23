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

import { deserializeVoidResponse } from '../../client/client.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { responseError } from '../error.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import { uint8ToBuf } from '../number.utils.js';
import { HeaderKind } from '../message/header.type.js';
import { HeaderValue } from '../message/header.utils.js';
import {
  OPTION_CATALOG, dedupeOptions, serializeOptions, sortOptions,
  type OptionDescriptor, type OptionEntry
} from '../options.utils.js';
import { OptionFlag, OptionsScope } from './describe-options.command.js';

const INVALID_COMMAND = 3;
const INVALID_USERNAME = 43;
const INVALID_USER_STATUS = 45;
const INVALID_STREAM_NAME = 1013;
const INVALID_TOPIC_NAME = 2014;
const UNSUPPORTED_OPTION_KEY = 4041;
const OPTION_NOT_UPDATABLE = 4103;
const OPTION_NOT_RESETTABLE = 4104;
const READ_ONLY_OPTION = 4105;
const MAX_NAME_LENGTH = 255;
const MAX_RESET_KEYS = 255;
const KEY_PREVIEW_LENGTH = 64;
const MIN_USERNAME_LENGTH = 3;
const MAX_USERNAME_LENGTH = 50;

/** The resource an update patches, tagged by its options scope. */
export type ResourceTarget =
  | { scope: typeof OptionsScope.Topic, streamId: Id, topicId: Id }
  | { scope: typeof OptionsScope.Stream, streamId: Id }
  | { scope: typeof OptionsScope.User, userId: Id };

/**
 * Parameters for the generic update: the keys to set, the keys to reset to
 * their defaults. Every other key keeps its stored value.
 *
 * Typed keys are checked here with the category and in the order the server
 * admits them. A patch carrying any raw entry or raw reset name gets only the
 * structural checks, because the server catalog may know keys or flags this
 * build does not, so the server decides the rest.
 */
export type UpdateResource = {
  target: ResourceTarget,
  /** Keys set through typed fields. A later entry for a key wins */
  options?: OptionEntry[],
  /** Keys reset through typed members */
  reset?: readonly string[],
  /** Entries with no typed field. A typed entry of the same key wins */
  rawOptions?: OptionEntry[],
  /** Reset names with no typed member */
  rawReset?: readonly string[]
};

/**
 * Update resource command definition, the one update for streams, topics and
 * users. Layout: `[scope:u8][identifiers][reset_count:u8][reset_key:name]*`
 * followed by the options block running to the end of the payload. Reset
 * names and set entries are both written in ascending name byte order.
 */
export const UPDATE_RESOURCE = {
  code: COMMAND_CODE.UpdateResource,

  serialize: ({
    target, options = [], reset = [], rawOptions = [], rawReset = []
  }: UpdateResource) => {
    const set = sortOptions(dedupeOptions([...rawOptions, ...options]));
    const resetKeys = [...new Set([...reset, ...rawReset])]
      .sort((left, right) => Buffer.compare(Buffer.from(left), Buffer.from(right)));
    checkPatch(target.scope, set, resetKeys, rawOptions.length > 0 || rawReset.length > 0);

    const identifiers = target.scope === OptionsScope.Topic
      ? [serializeIdentifier(target.streamId), serializeIdentifier(target.topicId)]
      : [serializeIdentifier(target.scope === OptionsScope.Stream ? target.streamId : target.userId)];

    return Buffer.concat([
      uint8ToBuf(target.scope),
      ...identifiers,
      uint8ToBuf(resetKeys.length),
      ...resetKeys.map((key) => {
        const name = Buffer.from(key);
        return Buffer.concat([uint8ToBuf(name.length), name]);
      }),
      serializeOptions(set)
    ]);
  },

  deserialize: deserializeVoidResponse
};

/**
 * A typed column value as an option entry. Text no header value can carry
 * travels as a bool, a kind the column never accepts, so the refusal is the
 * column's own error on either side.
 */
export const columnOption = (key: string, text: string): OptionEntry => {
  const length = Buffer.byteLength(text);
  return {
    key,
    value: length >= 1 && length <= MAX_NAME_LENGTH ? HeaderValue.String(text) : HeaderValue.Bool(false)
  };
};

/** Refuses a patch with the category and in the order the server admits it. */
const checkPatch = (scope: OptionsScope, set: OptionEntry[], reset: string[], raw: boolean): void => {
  const refuse = (code: number, detail: string): never => {
    throw responseError(COMMAND_CODE.UpdateResource, code, detail);
  };
  if (set.length === 0 && reset.length === 0)
    refuse(INVALID_COMMAND, 'the update sets and resets nothing');
  const conflict = reset.find((key) => set.some((entry) => entry.key === key));
  if (conflict !== undefined)
    refuse(INVALID_COMMAND, `key '${conflict}' is both set and reset`);
  const nameLength = (key: string) => Buffer.byteLength(key);
  // The catalog is far smaller than the count byte, so an overlong list always
  // names a key admission would refuse.
  const unwritable = reset.find((key) => nameLength(key) < 1 || nameLength(key) > MAX_NAME_LENGTH)
    ?? (reset.length > MAX_RESET_KEYS
      ? reset.find((key) => !OPTION_CATALOG.get(key)?.scopes.includes(scope))
      : undefined);
  if (unwritable !== undefined)
    refuse(UNSUPPORTED_OPTION_KEY, `reset of '${unwritable.slice(0, KEY_PREVIEW_LENGTH)}'`);
  if (raw)
    return;

  const keys = [...set.map((entry) => entry.key), ...reset];
  const outOfScope = keys.find((key) => !OPTION_CATALOG.get(key)?.scopes.includes(scope));
  if (outOfScope !== undefined)
    refuse(UNSUPPORTED_OPTION_KEY, `key '${outOfScope}'`);
  const first = (candidates: string[], refused: (descriptor: OptionDescriptor) => boolean) =>
    candidates.find((key) => {
      const descriptor = OPTION_CATALOG.get(key);
      return descriptor !== undefined && refused(descriptor);
    });
  const readOnly = first(keys, ({ flags, internal }) => internal || (flags & OptionFlag.ReadOnly) !== 0);
  if (readOnly !== undefined)
    refuse(READ_ONLY_OPTION, `key '${readOnly}'`);
  const createOnly = first(keys, ({ flags }) => (flags & OptionFlag.Updatable) === 0);
  if (createOnly !== undefined)
    refuse(OPTION_NOT_UPDATABLE, `key '${createOnly}'`);
  const fixed = first(reset, ({ flags }) => (flags & OptionFlag.Resettable) === 0);
  if (fixed !== undefined)
    refuse(OPTION_NOT_RESETTABLE, `reset of '${fixed}'`);

  for (const { key, value } of set) {
    const text = value.kind === HeaderKind.String ? value.value : undefined;
    const length = text === undefined ? 0 : Buffer.byteLength(text);
    if (key === 'name' && (length < 1 || length > MAX_NAME_LENGTH))
      refuse(scope === OptionsScope.Topic ? INVALID_TOPIC_NAME : INVALID_STREAM_NAME, 'name');
    if (key === 'username' && (length < MIN_USERNAME_LENGTH || length > MAX_USERNAME_LENGTH))
      refuse(INVALID_USERNAME, 'username');
    if (key === 'status' && text !== 'active' && text !== 'inactive')
      refuse(INVALID_USER_STATUS, 'status');
  }
};
