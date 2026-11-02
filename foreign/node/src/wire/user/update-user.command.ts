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
import { HeaderValue } from '../message/header.utils.js';
import type { OptionEntry, UserOptionKey } from '../options.utils.js';
import { OptionsScope } from '../system/describe-options.command.js';
import { UPDATE_RESOURCE, columnOption } from '../system/update-resource.command.js';
import { UserStatus } from './user.utils.js';


/**
 * Parameters for the update user command. Keys left out keep their values.
 */
export type UpdateUser = {
  /** User identifier (ID or username) */
  userId: Id,
  /** Optional new username (3-50 bytes) */
  username?: string,
  /** Optional new status. Deactivating a user ends its sessions. */
  status?: UserStatus,
  /**
   * Keys to reset to their defaults. No user key is resettable yet, so any key
   * is refused before the request is sent.
   */
  reset?: UserOptionKey[],
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
};

/**
 * Update user command definition.
 * Changes only the keys it names, through the generic update command.
 */
export const UPDATE_USER = {
  code: UPDATE_RESOURCE.code,

  serialize: ({ userId, username, status, reset, rawReset, options: rawOptions }: UpdateUser) => {
    const options: OptionEntry[] = [];
    if (username !== undefined)
      options.push(columnOption('username', username));
    if (status !== undefined)
      options.push(status === UserStatus.Active || status === UserStatus.Inactive
        ? { key: 'status', value: HeaderValue.String(status === UserStatus.Active ? 'active' : 'inactive') }
        : { key: 'status', value: HeaderValue.Bool(false) });
    return UPDATE_RESOURCE.serialize({
      target: { scope: OptionsScope.User, userId }, options, reset, rawOptions, rawReset
    });
  },

  deserialize: UPDATE_RESOURCE.deserialize
};


/**
 * Executable update user command function.
 */
export const updateUser = wrapCommand<UpdateUser, boolean>(UPDATE_USER);
