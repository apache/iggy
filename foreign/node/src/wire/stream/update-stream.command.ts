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

import { wrapCommand } from '../command.utils.js';
import type { Id } from '../identifier.utils.js';
import type { OptionEntry, StreamOptionKey } from '../options.utils.js';
import { OptionsScope } from '../system/describe-options.command.js';
import { UPDATE_RESOURCE, columnOption } from '../system/update-resource.command.js';

/**
 * Parameters for the update stream command. Keys left out keep their values.
 */
export type UpdateStream = {
  /** Stream identifier (ID or name) */
  streamId: Id,
  /** New stream name (1-255 bytes) */
  name?: string,
  /**
   * Keys to reset to their defaults. No stream key is resettable yet, so any
   * key is refused before the request is sent.
   */
  reset?: StreamOptionKey[],
  /**
   * Reset names with no typed member, for a resettable key the server catalog
   * gained after this build shipped. The server decides on them.
   */
  rawReset?: readonly string[],
  /**
   * Option keys with no field of their own, for an updatable key the server
   * catalog gained after this build shipped. A field above wins on collision.
   * The server decides on them, so a create-only or unknown key comes back as
   * a server error.
   */
  options?: OptionEntry[]
}

/**
 * Update stream command definition.
 * Changes only the keys it names, through the generic update command.
 */
export const UPDATE_STREAM = {
  code: UPDATE_RESOURCE.code,

  serialize: ({ streamId, name, reset, rawReset, options: rawOptions }: UpdateStream) => {
    const options = name === undefined ? [] : [columnOption('name', name)];
    return UPDATE_RESOURCE.serialize({
      target: { scope: OptionsScope.Stream, streamId }, options, reset, rawOptions, rawReset
    });
  },

  deserialize: UPDATE_RESOURCE.deserialize
};


/**
 * Executable update stream command function.
 */
export const updateStream = wrapCommand<UpdateStream, boolean>(UPDATE_STREAM);
