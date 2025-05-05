
import type { RawClient, ClientConfig } from "./client.type.js"
import { createPool, type Pool } from 'generic-pool';
import { CommandAPI } from '../wire/command-set.js';
import { TcpClient } from './tcp.client.js';
import { TlsClient } from './tls.client.js';
import { debug } from './client.debug.js';


export const rawClientGetter = (config: ClientConfig): Promise<RawClient> => {
  const { transport, options } = config;
  switch (transport) {
    case 'TLS': return TlsClient(options);
    case 'TCP':
    default:
      return TcpClient(options);
  }
}

// create & destroy must be async
const createPoolFactory = (config: ClientConfig) => ({
  create: function () {
    return rawClientGetter(config);
  },
  destroy: async function (client: RawClient) {
    return client.destroy();
  }
});

export class Client extends CommandAPI {
  _config: ClientConfig
  _pool: Pool<RawClient>
  destroy: () => void
  
  constructor(config: ClientConfig) {
    const min = config.poolSize?.min || 1;
    const max = config.poolSize?.max || 4;
    const pool = createPool(createPoolFactory(config), { min, max });
    const getFromPool = async () => {
      const c = await pool.acquire();
      if (!c.isAuthenticated)
        await c.authenticate(config.credentials);
      debug('client acquired from pool. pool size is', pool.size);
      c.once('finishQueue', () => {
        pool.release(c)
        debug('client released to pool. pool size is', pool.size);
      });
      return c;
    };
    super(getFromPool);
    this._config = config;
    this._pool = pool;
    this.destroy = async () => {
      debug('destroying client pool. pool size is', pool.size);
      await this._pool.drain();
      await this._pool.clear();
      debug('destroyed client pool. pool size is', pool.size);
    }
  };
}

export class SingleClient extends CommandAPI {
  _config: ClientConfig
  destroy: () => void
  
  constructor(config: ClientConfig) {
    const cliP = rawClientGetter(config);
    const init = async () => {
      const c = await cliP;
      if (!c.isAuthenticated)
        await c.authenticate(config.credentials);
      return c;
    };
    super(init);
    this._config = config;
    this.destroy = async () => {
      const s = await this.clientProvider();
      s.destroy();
    };
  }
};


export class SimpleClient extends CommandAPI {
  constructor(client: RawClient) {
    super(() => Promise.resolve(client));
  }
};

export const getClient = async (config: ClientConfig) => {
  const cli = await rawClientGetter(config);
  if (!cli.isAuthenticated)
    await cli.authenticate(config.credentials);
  const api = new SimpleClient(cli);
  return api;
};
