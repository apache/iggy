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
import { wrapCommand } from '../command.utils.js';
import { COMMAND_CODE } from '../command.code.js';
import { uint8ToBuf } from '../number.utils.js';

/**
 * Parameters for the retire geo origin command.
 */
export type RetireGeoOrigin = {
  /** Name of the geo peer that never returns, such as `dc-a` (1-255 bytes) */
  origin: string
};

/**
 * Retire geo origin command definition.
 * Makes this cluster stop following a geo peer that never returns. Needs the
 * root user or the `manage_servers` permission.
 */
export const RETIRE_GEO_ORIGIN = {
  code: COMMAND_CODE.RetireGeoOrigin,

  serialize: ({ origin }: RetireGeoOrigin): Buffer => {
    const name = Buffer.from(origin);

    if (name.length < 1 || name.length > 255)
      throw new Error('Geo origin name should be between 1 and 255 bytes');

    return Buffer.concat([uint8ToBuf(name.length), name]);
  },

  deserialize: deserializeVoidResponse
};

/**
 * Executable retire geo origin command function.
 */
export const retireGeoOrigin = wrapCommand<RetireGeoOrigin, boolean>(RETIRE_GEO_ORIGIN);
