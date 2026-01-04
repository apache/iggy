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


import { deserializeVoidResponse } from '../../client/client.utils.js';
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { uint8ToBuf } from '../number.utils.js';

/**
 * Parameters for the delete access token command.
 */
export type DeleteToken = {
  /** Token name (1-255 bytes) */
  name: string
};

/**
 * Delete access token command definition.
 * Removes an existing access token.
 */
export const DELETE_TOKEN = {
  code: COMMAND_CODE.DeleteAccessToken,

  serialize: ({name}: DeleteToken): Buffer => {
    const bName = Buffer.from(name);

    if (bName.length < 1 || bName.length > 255)
      throw new Error('Token name should be between 1 and 255 bytes');

    const b = uint8ToBuf(bName.length);
    return Buffer.concat([
      b,
      bName
    ]);
  },

  deserialize: deserializeVoidResponse
};

/**
 * Executable delete access token command function.
 */
export const deleteToken = wrapCommand<DeleteToken, boolean>(DELETE_TOKEN);
