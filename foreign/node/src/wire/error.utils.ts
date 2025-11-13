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


import { translateCommandCode } from './command.code.js';
import { translateErrorCode } from './error.code.js';

export const responseError = (cmdCode: number, errCode: number) => new Error(
  `command: { code: ${cmdCode}, name: ${translateCommandCode(cmdCode)} } ` +
  `error: {code: ${errCode}, message: ${translateErrorCode(errCode)} }`
);

export class DeserializeError extends Error {
  constructor(message: string, cause?: Record<string , unknown>) {
    super(message, cause);
    this.name = "DeserializeError";
    Object.setPrototypeOf(this, DeserializeError.prototype);
  }
}

export const deserializeError = (
  name: string,
  position: number,
  buffer_size: number,
  lookup?: number,
  read?: Record<string, unknown>
) => {
  throw new DeserializeError(
    `cannot deserialize ${name}: buffer too small`,
    { cause: { position, buffer_size, lookup, read } }
  );
}
