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


import type { CommandResponse, ClientProvider } from '../client/client.type.js';

/**
 * Represents a command that can be sent to the Iggy server.
 *
 * @typeParam I - Input type for the command arguments
 * @typeParam O - Output type for the command response
 */
export type Command<I, O> = {
  /** Command code identifying the operation */
  code: number,
  /** Function to serialize command arguments to a Buffer */
  serialize: (args: I) => Buffer,
  /** Function to deserialize the server response */
  deserialize: (r: CommandResponse) => O
}


/**
 * Wraps a command definition into an executable function.
 * Creates a function that handles serialization, sending, and deserialization.
 *
 * @typeParam I - Input type for the command arguments
 * @typeParam O - Output type for the command response
 * @param cmd - Command definition with code, serialize, and deserialize functions
 * @returns A function that takes a ClientProvider and returns an async command executor
 */
export function wrapCommand<I, O>(cmd: Command<I, O>) {
  return (getClient: ClientProvider) =>
    async (arg: I) => cmd.deserialize(
      await (await getClient()).sendCommand(cmd.code, cmd.serialize(arg))
    );
};
