
import type { Readable } from 'stream';
import type { TcpOption } from './tcp.client.js';
import type { TlsOption } from './tls.client.js';

export type CommandResponse = {
  status: number,
  length: number,
  data: Buffer
};

export type RawClient = {
  sendCommand: (code: number, payload: Buffer, handleResponse?: boolean) => Promise<CommandResponse>,
  destroy: () => void,
  isAuthenticated: boolean
  authenticate: (c: ClientCredentials) => Promise<boolean>
  on: (ev: string, cb: () => void) => void
  once: (ev: string, cb: () => void) => void
  getReadStream: () => Readable
}

export type ClientProvider = () => Promise<RawClient>;

export const Transports = ['TCP', 'TLS' /**, 'QUIC' */] as const;
export type TransportType = typeof Transports[number];

type TransportOption = TcpOption | TlsOption;

export type TokenCredentials = {
  token: string
}

export type PasswordCredentials = {
  username: string,
  password: string
}

export type ClientCredentials = TokenCredentials | PasswordCredentials;

export type PoolSizeOption = {
  min?: number,
  max?: number
}

export type ClientConfig = {
  transport: TransportType,
  options: TransportOption,
  credentials: ClientCredentials,
  poolSize?: PoolSizeOption
}
