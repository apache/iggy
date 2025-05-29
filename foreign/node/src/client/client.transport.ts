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
import { createConnection, type Socket } from 'node:net';
import { connect as TLSConnect } from 'node:tls';
import { debug } from './client.debug.js';
import type { ClientConfig, TlsOption, TcpOption, ReconnectOption } from './client.type.js';


const createTcpSocket = (options: TcpOption): Socket => {
  return createConnection(options);
};

const createTlsSocket = ({ port, ...options }: TlsOption): Socket => {
  const socket = TLSConnect(port, options);
  socket.setEncoding('utf8');
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

function recreate(option: ClientConfig, timer=1000): Promise<Socket> {
  return new Promise((resolve) => {
    setTimeout(() => {
      resolve(getTransport(option));
    }, timer);
  });
}

export class IggyTransport extends EventEmitter {
  public socket: Socket;
  public connected: boolean;
  public connecting: boolean;
  private config: ClientConfig;
  private reconnectOption: ReconnectOption;
  private reconnectCount: number;
  private lastError: unknown[];

  constructor(config: ClientConfig) {
    super()
    this.config = config;
    this.reconnectOption = { ...DefaultReconnectOption, ...config.reconnect };
    this.reconnectCount = 0;
    this.connected = false;
    this.connecting = true;
    this.socket = getTransport(config);
    this.wrapSocket();
    this.lastError = [];
  }

  wrapSocket() {
    this.socket.on('connect', () => {
      debug('socket#connect event');
      this.connected = true;
      this.connecting = false;
      // const lastReconnectCount = this.reconnectCount;
      this.reconnectCount = 0;
      this.lastError = [];
      this.emit('connect');
      // if(lastReconnectCount > 0)
      //   this.emit('connect');
    });

    this.socket.once('close', async (hadError?: boolean) => {
      this.connected = false;
      this.emit('disconnected', hadError);
      const { enabled, interval, maxRetries } = this.reconnectOption
      debug(
        'socket#close event',
        {
          hadError,
          reconnect: { enabled, interval, maxRetries },
          count:this.reconnectCount
        }
      );
      if(!enabled)
        return this.emit('close', hadError);

      if(this.reconnectCount > maxRetries) {
        debug(`reconnect reached maxRetries of ${ maxRetries }`, this.lastError);
        this.emit('close', hadError);
        this.emit(
          'error',
          new Error(
            `reconnect maxRetries exceeded (count: ${ this.reconnectCount })`,
            { cause: this.lastError.pop() }
          ));
        return;
      }

      this.connecting = true;
      this.reconnectCount += 1;
      this.socket = await recreate(this.config, interval);
      this.wrapSocket();      
      // this.emit('close');
    });
    
    this.socket.on('end', async (err: unknown) => {
      console.error('Transport/socket#end', err);
      this.emit('end');
    });

    this.socket.on('drain', () => this.emit('drain'));

    this.socket.on('error', (err) => {
      console.error('Transport/socket#error !1/2-noemit', err);
      this.lastError.push(err);
      if(!this.connecting && !this.connected)
        this.emit('error', err);
    });

    this.socket.on(
      'lookup',
      (err, address, family, host) => this.emit('lookup', err, address, family, host)
    );
    this.socket.on('ready', () => this.emit('ready'));
    this.socket.on('timeout', () => this.emit('timeout'));
    // customize data events
    this.socket.on('readable', () => this.emit('readable'));
  };

};
