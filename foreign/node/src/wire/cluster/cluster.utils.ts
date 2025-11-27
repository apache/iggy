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

import { deserializeError } from '../error.utils.js';

import {
  type ClusterNodeRoleKey,
  type ClusterNodeStatusKey,
  mapClusterNodeRole,
  mapClusterNodeStatus,
} from './cluster.type.js';


export type TransportEndpoints = {
  tcp: number
  quic: number
  http: number
  websocket: number
}

export type ClusterNode = {
  name: string
  ip: string
  endpoints: TransportEndpoints
  role: ClusterNodeRoleKey
  status: ClusterNodeStatusKey
}

type DeserializedClusterNode = { length: number, data: ClusterNode }

const CLUSTER_NODE_MIN_BUFFER_SIZE = 18; // 4 + 1 + 4 + 1 + 8 (endpoints) + 1 + 1;

export const deserializeNode = (b: Buffer, pos = 0): DeserializedClusterNode => {
  if (b.length - pos < CLUSTER_NODE_MIN_BUFFER_SIZE)
    deserializeError(
      'cluster node', pos, b.length, CLUSTER_NODE_MIN_BUFFER_SIZE
    );

  const startPos = pos;

  // Read name
  const nameLen = b.readUInt32LE(pos);
  if (b.length - pos < 4 + nameLen)
    deserializeError(
      'cluster node', pos, b.length, 4 + nameLen, { nameLen }
    );

  const name = b.subarray(pos + 4, pos + 4 + nameLen).toString();
  pos += nameLen + 4;

  // Read IP
  const ipLen = b.readUInt32LE(pos);
  if (b.length - pos < 4 + ipLen)
    deserializeError(
      'cluster node', pos, b.length, 4 + ipLen, { nameLen, name, ipLen }
    );

  const ip = b.subarray(pos + 4, pos + 4 + ipLen).toString();
  pos += ipLen + 4;

  // Read transport endpoints (4 ports, each u16)
  if (b.length - pos < 8)
    deserializeError(
      'cluster node endpoints', pos, b.length, 8, { nameLen, name, ipLen, ip }
    );

  const endpoints: TransportEndpoints = {
    tcp: b.readUInt16LE(pos),
    quic: b.readUInt16LE(pos + 2),
    http: b.readUInt16LE(pos + 4),
    websocket: b.readUInt16LE(pos + 6)
  };
  pos += 8;

  // Read role and status
  if (b.length - pos < 2)
    deserializeError(
      'cluster node', pos, b.length, 2, { nameLen, name, ipLen, ip }
    );

  const role = mapClusterNodeRole(b.readUInt8(pos));
  pos += 1;

  const status = mapClusterNodeStatus(b.readUInt8(pos));
  pos += 1;

  return {
    length: pos - startPos,
    data: {
      name,
      ip,
      endpoints,
      role,
      status
    }
  }
};

export type ClusterMetadata = {
  // Name of the cluster.
  name: string,
  // List of all nodes in the cluster.
  nodes: ClusterNode[],
};

// min = 4 + 1 + 4 + CLUSTER_NODE_MIN_BUFFER_SIZE;
const CLUSTER_METADATA_MIN_SIZE = 8 + CLUSTER_NODE_MIN_BUFFER_SIZE;

export const deserializeMetadata = (b: Buffer, pos = 0): ClusterMetadata => {
  if (b.length - pos < CLUSTER_METADATA_MIN_SIZE)
    deserializeError(
      'cluster metadata', pos, b.length, CLUSTER_METADATA_MIN_SIZE
    );

  // Read cluster name
  const nameLen = b.readUInt32LE(pos);
  if (b.length - pos < 4 + nameLen)
    deserializeError(
      'cluster metadata', pos, b.length, 4 + nameLen, { nameLen }
    );

  const name = b.subarray(pos + 4, pos + 4 + nameLen).toString();
  pos += nameLen + 4;

  // Read nodes count
  if (b.length - pos < 4)
    deserializeError(
      'cluster metadata', pos, b.length, 4, { nameLen, name }
    );

  const nodeCount = b.readUInt32LE(pos);
  pos += 4;

  // Read nodes array
  const nodes: ClusterNode[] = [];
  for (let i = 0; i < nodeCount; i++) {
    const { length, data } = deserializeNode(b, pos);
    nodes.push(data);
    pos += length;
  }

  return {
    name,
    nodes
  }
};
