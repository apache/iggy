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


import { createConnection, type TcpSocketConnectOpts } from 'node:net';
import type { RawClient } from './client.type.js';
import { wrapSocket, type CommandResponseStream } from './client.socket.js';

export const createTcpSocket =
  (options: TcpSocketConnectOpts): Promise<CommandResponseStream> => {
    const socket = createConnection(options);
    return wrapSocket(socket);
  };


export type TcpOption = TcpSocketConnectOpts;

export const TcpClient = ({ host, port, keepAlive = true }: TcpOption): Promise<RawClient> =>
  createTcpSocket({ host, port, keepAlive });
