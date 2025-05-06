
import { connect, type ConnectionOptions } from 'node:tls';
import type { RawClient } from './client.type.js';
import { wrapSocket, type CommandResponseStream } from './client.socket.js';

export const createTlsSocket = (
  port: number, options: ConnectionOptions
): Promise<CommandResponseStream> => {
  const socket = connect(port, options);
  socket.setEncoding('utf8');
  return wrapSocket(socket);
}

export type TlsOption = { port: number } & ConnectionOptions;

export const TlsClient = ({ port, ...options }: TlsOption): Promise<RawClient> => {
  return createTlsSocket(port, options);
};
