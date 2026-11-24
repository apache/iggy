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


import { describe, it } from 'node:test';
import assert from 'node:assert/strict';
import {
  ClusterNodeRole,
  ClusterNodeStatus,
} from './cluster.type.js';
import {
  deserializeMetadata,
  deserializeNode,
  type ClusterNode,
  type TransportEndpoints,
} from './cluster.utils.js';

const u32 = (n: number) => {
  const b = Buffer.alloc(4);
  b.writeUInt32LE(n);
  return b;
};

const buildNode = (
  {
    replicaId = 1,
    name = 'node-1',
    ip = '127.0.0.1',
    endpoints = { tcp: 8090, quic: 8091, http: 8092, websocket: 8093 },
    role = ClusterNodeRole.Leader,
    status = ClusterNodeStatus.Healthy,
  }: Partial<Omit<ClusterNode, 'endpoints'>> & { endpoints?: TransportEndpoints } = {}
): Buffer => {
  const nameBuf = Buffer.from(name);
  const ipBuf = Buffer.from(ip);
  const endpointsBuf = Buffer.alloc(8);
  endpointsBuf.writeUInt16LE(endpoints.tcp, 0);
  endpointsBuf.writeUInt16LE(endpoints.quic, 2);
  endpointsBuf.writeUInt16LE(endpoints.http, 4);
  endpointsBuf.writeUInt16LE(endpoints.websocket, 6);

  return Buffer.concat([
    Buffer.from([replicaId]),
    u32(nameBuf.length),
    nameBuf,
    u32(ipBuf.length),
    ipBuf,
    endpointsBuf,
    Buffer.from([role, status]),
  ]);
};

describe('cluster utils', () => {

  describe('deserializeNode', () => {

    it('decodes every field of a hand-built node', () => {
      const buffer = buildNode({
        replicaId: 3,
        name: 'node-3',
        ip: '10.0.0.3',
        endpoints: { tcp: 1, quic: 2, http: 3, websocket: 4 },
        role: ClusterNodeRole.Follower,
        status: ClusterNodeStatus.Starting,
      });

      const { length, data } = deserializeNode(buffer);

      assert.equal(length, buffer.length);
      assert.deepEqual(data, {
        replicaId: 3,
        name: 'node-3',
        ip: '10.0.0.3',
        endpoints: { tcp: 1, quic: 2, http: 3, websocket: 4 },
        role: 'Follower',
        status: 'Starting',
      });
    });

    it('maps every valid role byte', () => {
      for (const [name, value] of Object.entries(ClusterNodeRole)) {
        const { data } = deserializeNode(buildNode({ role: value }));
        assert.equal(data.role, name);
      }
    });

    it('maps every valid status byte', () => {
      for (const [name, value] of Object.entries(ClusterNodeStatus)) {
        const { data } = deserializeNode(buildNode({ status: value }));
        assert.equal(data.status, name);
      }
    });

    it('throws for a buffer shorter than the minimum node', () => {
      assert.throws(
        () => deserializeNode(Buffer.alloc(18)),
        /cluster node/
      );
    });

    it('throws when the name length runs past the end', () => {
      const buffer = Buffer.concat([Buffer.from([1]), u32(100), Buffer.from('short')]);
      assert.throws(() => deserializeNode(buffer), /cluster node/);
    });

    it('throws for an unknown role byte', () => {
      assert.throws(
        () => deserializeNode(buildNode({ role: 9 })),
        /Invalid ClusterNodeRole/
      );
    });

  });

  describe('deserializeMetadata', () => {

    it('decodes a cluster name and two nodes', () => {
      const name = Buffer.from('cluster-a');
      const first = buildNode({ replicaId: 1, name: 'node-1' });
      const second = buildNode({
        replicaId: 2,
        name: 'node-2',
        role: ClusterNodeRole.Follower,
        status: ClusterNodeStatus.Maintenance,
      });
      const buffer = Buffer.concat([
        u32(name.length),
        name,
        u32(2),
        first,
        second,
      ]);

      const metadata = deserializeMetadata(buffer);

      assert.equal(metadata.name, 'cluster-a');
      assert.equal(metadata.nodes.length, 2);
      assert.equal(metadata.nodes[0].name, 'node-1');
      assert.equal(metadata.nodes[1].name, 'node-2');
      assert.equal(metadata.nodes[1].status, 'Maintenance');
    });

  });

});
