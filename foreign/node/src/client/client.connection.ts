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


import { EventEmitter } from 'node:events';
import type { Socket } from 'node:net';
import { createConnection } from 'node:net';
import { connect as TLSConnect } from 'node:tls';
import type { ClientConfig, TlsOption, TcpOption, ReconnectOption } from "./client.type.js"
import { serializeCommand } from './client.utils.js';
import { debug } from './client.debug.js';


const createTcpSocket = (options: TcpOption): Socket => {
  return createConnection(options);
};

const createTlsSocket = ({ port, ...options }: TlsOption): Socket => {
  const socket = TLSConnect(port, options);
  return socket;
};

const getTransport = (config: ClientConfig): Socket => {
  const { transport, options } = config;
  switch (transport) {
    case 'TLS': return createTlsSocket(options);
    case 'TCP':
    default:
      return createTcpSocket(options);
  }
};

const DefaultReconnectOption: ReconnectOption = {
  enabled: true,
  interval: 5 * 1000,
  maxRetries: 12
}

function recreate(option: ClientConfig, timer = 1000): Promise<Socket> {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(getTransport(option));
    }, timer);
  });
}

type SocketError = Error & { code?: string };

export class IggyConnection extends EventEmitter {
  public config: ClientConfig
  public socket: Socket;

  public connected: boolean;
  public connecting: boolean;
  public ending: boolean;
  private waitingResponseEnd: boolean;
  private reconnectOption: ReconnectOption;
  private reconnectCount: number;

  private readBuffers: Buffer;

  constructor(config: ClientConfig) {
    super();
    this.config = config;
    this.socket = getTransport(config);
    this.connected = false;
    this.connecting = false;
    this.ending = false;
    this.waitingResponseEnd = false;
    this.reconnectOption = { ...DefaultReconnectOption, ...config.reconnect };
    this.reconnectCount = 0;
    this.readBuffers = Buffer.allocUnsafe(0);
  }

  connect() {
    this.connecting = true;

    this.socket.on('data', this._onData.bind(this));

    this.socket.on('error', async (err: SocketError) => {
      debug('socket/error event', err, err.code, this.ending);
      // errors about disconnections should be ignored during disconnect
      if (this.ending && (err?.code === 'ECONNRESET' || err?.code === 'EPIPE'))
        return

      this.reconnect(err);
    });

    this.socket.once('end', async (hadError?: boolean) => {
      debug('socket/close#END event', hadError);
      this.connected = false;
      this.emit('disconnected', hadError);
      this.reconnect();
    });


    return new Promise((resolve /**, reject*/) => {
      this.socket.once('connect', () => {
        debug('socket/connect event');
        this.connected = true;
        this.connecting = false;
        this.reconnectCount = 0;
        this.emit('connect');
        resolve(this);
      });
    });
  }

  async reconnect(err?: Error) {
    const { enabled, interval, maxRetries } = this.reconnectOption
    debug(
      'reconnect# event/reconnect?', {
      reconnect: { enabled, interval, maxRetries },
      count: this.reconnectCount,
      lastError: err
    }
    );

    if (!enabled || this.reconnectCount > maxRetries) {
      debug(`reconnect reached maxRetries of ${maxRetries}`, err);
      return this.emit(
        'error',
        new Error(
          `reconnect maxRetries exceeded (count: ${this.reconnectCount})`,
          { cause: err }
        ));
    }

    /** recreate socket */
    this.connecting = true;
    this.reconnectCount += 1;
    this.socket = await recreate(this.config, interval);
    this.connect();
  }

  _destroy() {
    this.ending = true;
    this.socket.destroy();
  }

  _endResponseWait() {
    this.readBuffers = Buffer.allocUnsafe(0);
    this.waitingResponseEnd = false;
  }

  _onData(data: Buffer) {
    debug(
      'ONDATA',
      typeof data,
      Buffer.isBuffer(data),
      data?.length,
      this.waitingResponseEnd
    );

    if (this.waitingResponseEnd) {
      data = Buffer.concat([this.readBuffers, data]);
    }

    if (data.length < 8) {
      this.socket.unshift(data);
      return;
    }

    /**
     * data[0:4] hold response status code
     * const status = data.readUInt32LE(0);
     * data[4:8] hold response length
     */
    const responseSize = data.readUInt32LE(4);

    /** response is just head, no payload */
    if (responseSize === 0 && data.length === 8) {
      this.emit('response', data);
      return;
    }

    const payloadLength = data.length - 8;

    /** payload is complete */
    if (payloadLength === responseSize) {
      this.emit('response', data);
      this._endResponseWait();
      return;
    }

    /** payload is incomplete, buffering until next data event */
    if (payloadLength < responseSize) {
      this.waitingResponseEnd = true;
      this.readBuffers = data;
      return;
    }

    /** payload is bigger than expected, cut response and unshift extra data  */
    if (payloadLength > responseSize) {
      this.emit('response', data.subarray(0, 8 + responseSize));
      this._endResponseWait();
      this.socket.unshift(data.subarray(8 + responseSize));
    }
  }

  writeCommand(command: number, payload: Buffer): boolean {
    const cmd = serializeCommand(command, payload);
    return this.socket.write(cmd);
  }
}
