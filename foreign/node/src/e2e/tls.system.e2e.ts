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

// TLS integration tests for the Node.js SDK.
//
// These tests require a TLS-enabled Iggy server and are skipped by default.
// To run them locally:
//
//   1. Start the server with TLS:
//        IGGY_ROOT_USERNAME=iggy IGGY_ROOT_PASSWORD=iggy \
//        IGGY_TCP_TLS_ENABLED=true \
//        IGGY_TCP_TLS_CERT_FILE=core/certs/iggy_cert.pem \
//        IGGY_TCP_TLS_KEY_FILE=core/certs/iggy_key.pem \
//        cargo r --bin iggy-server
//
//   2. Run the tests:
//        cd foreign/node
//        IGGY_TCP_TLS_ENABLED=true IGGY_TCP_ADDRESS=127.0.0.1:8090 \
//        node --import @swc-node/register/esm-register --test src/e2e/tls.system.e2e.ts

import { readFileSync } from 'node:fs';
import { resolve } from 'node:path';
import { after, describe, it } from 'node:test';
import assert from 'node:assert/strict';
import { Client } from '../client/client.js';
import { Partitioning, Consumer, PollingStrategy } from '../wire/index.js';
import { getIggyAddress } from '../tcp.sm.utils.js';

const tlsEnabled = process.env.IGGY_TCP_TLS_ENABLED === 'true';

// Path to the CA certificate. Override with E2E_ROOT_CA_CERT env var,
// otherwise fall back to the default relative path from the repo root.
const caCertPath = process.env.E2E_ROOT_CA_CERT
  || resolve(process.cwd(), '../../core/certs/iggy_ca_cert.pem');

const getTlsClient = () => {
  const [host, port] = getIggyAddress();
  const caCert = readFileSync(caCertPath);

  // The server certificate is issued for 'localhost'. When IGGY_TCP_ADDRESS uses
  // an IP (e.g. 127.0.0.1), the default TLS hostname check would fail because
  // the cert CN/SAN does not match an IP literal. Providing a custom
  // checkServerIdentity that always succeeds works around this for local testing.
  return new Client({
    transport: 'TLS',
    options: {
      port,
      host,
      ca: caCert,
      checkServerIdentity: () => undefined,
    },
    credentials: { username: 'iggy', password: 'iggy' },
  });
};

describe('e2e -> tls', { skip: !tlsEnabled && 'IGGY_TCP_TLS_ENABLED is not set' }, async () => {

  const c = getTlsClient();
  const credentials = { username: 'iggy', password: 'iggy' };

  it('e2e -> tls::ping', async () => {
    assert.ok(await c.system.ping());
  });

  it('e2e -> tls::login', async () => {
    assert.deepEqual(
      await c.session.login(credentials),
      { userId: 0 }
    );
  });

  it('e2e -> tls::getStats', async () => {
    const stats = await c.system.getStats();
    assert.ok(stats);
    assert.ok('processId' in stats);
    assert.ok('hostname' in stats);
  });

  it('e2e -> tls::send and poll messages', async () => {
    const streamName = 'tls-e2e-stream';
    const topicName = 'tls-e2e-topic';

    await c.stream.create({ name: streamName });
    await c.topic.create({
      streamId: streamName,
      name: topicName,
      partitionCount: 1,
      compressionAlgorithm: 1,
    });

    const messages = [
      { id: 1, headers: [], payload: 'tls-message-1' },
      { id: 2, headers: [], payload: 'tls-message-2' },
      { id: 3, headers: [], payload: 'tls-message-3' },
    ];

    await c.message.send({
      streamId: streamName,
      topicId: topicName,
      messages,
      partition: Partitioning.PartitionId(0),
    });

    const polled = await c.message.poll({
      streamId: streamName,
      topicId: topicName,
      consumer: Consumer.Single,
      partitionId: 0,
      pollingStrategy: PollingStrategy.First,
      count: 10,
      autocommit: false,
    });

    assert.equal(polled.messages.length, 3);

    await c.stream.delete({ streamId: streamName });
  });

  it('e2e -> tls::logout', async () => {
    assert.ok(await c.session.logout());
  });

  after(() => {
    c.destroy();
  });
});
