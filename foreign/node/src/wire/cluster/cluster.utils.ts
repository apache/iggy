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
  type TransportProtocolKey,
  mapClusterNodeRole,
  mapClusterNodeStatus,
  mapTransportProtocol,
} from './cluster.type.js';


export type ClusterNode = {
  id: number
  name: string
  address: string
  role: ClusterNodeRoleKey
  status: ClusterNodeStatusKey
}

type DeserializedClusterNode = { length: number, data: ClusterNode }

const CLUSTER_NODE_MIN_BUFFER_SIZE = 16; // 4 + 1 + 4 + 1 + 4 + 1 + 1;

export const deserializeNode = (b: Buffer, pos = 0): DeserializedClusterNode => {
  if (b.length - pos < CLUSTER_NODE_MIN_BUFFER_SIZE)
    deserializeError(
      'cluster node', pos, b.length, CLUSTER_NODE_MIN_BUFFER_SIZE
    );

  const id = b.readUInt32LE(pos);
  pos += 4;

  const nameLen = b.readUInt32LE(pos);
  if (b.length - pos < 4 + nameLen)
    deserializeError(
      'cluster node', pos, b.length, 4 + nameLen, { id, nameLen }
    );

  const name = b.subarray(pos + 4, pos + 4 + nameLen).toString();
  pos += nameLen + 4;

  const addrLen = b.readUInt32LE(pos);
  if (b.length - pos < 4 + addrLen)
    deserializeError(
      'cluster node', pos, b.length, 4 + addrLen, { id, nameLen, name, addrLen }
    );

  const address = b.subarray(pos + 4, pos + 4 + addrLen).toString();
  pos += addrLen + 4;

  if (b.length - pos < 2)
    deserializeError(
      'cluster node', pos, b.length, 2, { id, nameLen, name, addrLen, address }
    );

  const role = mapClusterNodeRole(b.readUInt8(pos));
  pos += 1;

  const status = mapClusterNodeStatus(b.readUInt8(pos));
  pos += 1;

  return {
    length: 4 + nameLen + 4 + addrLen + 4 + 1 + 1,
    data: {
      id,
      name,
      address,
      role,
      status
    }
  }
};

export type ClusterMetadata = {
  // Name of the cluster.
  name: string,
  // Unique identifier of the cluster.
  id: number,
  // Transport used for cluster communication
  // (for binary protocol it's u8, 1=TCP, 2=QUIC, 3=HTTP).
  transport: TransportProtocolKey,
  // List of all nodes in the cluster.
  nodes: ClusterNode[],
};

// min = 4 + 1 + 4 + 1 + 1 + 4 + CLUSTER_NODE_MIN_BUFFER_SIZE;
const CLUSTER_METADATA_MIN_SIZE = 15 + CLUSTER_NODE_MIN_BUFFER_SIZE;

export const deserializeMetadata = (b: Buffer, pos = 0): ClusterMetadata => {
  if (b.length - pos < CLUSTER_METADATA_MIN_SIZE)
    deserializeError(
      'cluster metadata', pos, b.length, CLUSTER_METADATA_MIN_SIZE
    );

  const nameLen = b.readUInt32LE(pos);
  if (b.length - pos < 4 + nameLen)
    deserializeError(
      'cluster metadata', pos, b.length, 4 + nameLen, { nameLen }
    );

  const name = b.subarray(pos + 4, pos + 4 + nameLen).toString();
  pos += nameLen + 4;

  if (b.length - pos < 4 + 1 + 4)
    deserializeError(
      'cluster metadata', pos, b.length, 4 + 1 + 4, { nameLen, name }
    );

  const id = b.readUInt32LE(pos);
  pos += 4;

  const transport = mapTransportProtocol(b.readUInt8(pos));
  pos += 1;

  const nodeCount = b.readUInt32LE(pos);
  pos += 4;

  const nodes: ClusterNode[] = [];
  for (let i = 0; i < nodeCount; i++) {
    const { length, data } = deserializeNode(b, pos);
    nodes.push(data);
    pos += length;
  }

  return {
    name,
    id,
    transport,
    nodes
  }
};
