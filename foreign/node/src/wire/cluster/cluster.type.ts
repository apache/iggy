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

import { type ValueOf, reverseRecord } from "../../type.utils.js";

export const ClusterNodeRole = {
  Leader: 0,
  Follower: 1
} as const;

export type ClusterNodeRole = typeof ClusterNodeRole;
export type ClusterNodeRoleKey = keyof ClusterNodeRole;
export type ClusterNodeRoleValue = ValueOf<ClusterNodeRole>;
export const ReverseClusterNodeRole = reverseRecord(ClusterNodeRole);

export const ClusterNodeStatus = {
  // Node is healthy and responsive
  Healthy: 0,
  // Node is starting up
  Starting: 1,
  // Node is shutting down
  Stopping: 2,
  // Node is unreachable
  Unreachable: 3,
  // Node is in maintenance mode
  Maintenance: 4,
} as const;

export type ClusterNodeStatus = typeof ClusterNodeStatus;
export type ClusterNodeStatusKey = keyof ClusterNodeStatus;
export type ClusterNodeStatusValue = ValueOf<ClusterNodeStatus>;
export const ReverseClusterNodeStatus = reverseRecord(ClusterNodeStatus);


export const TransportProtocol = {
  TCP: 1,
  QUIC: 2,
  HTTP: 3,
  WebSocket: 4
} as const;

export type TransportProtocol = typeof TransportProtocol;
export type TransportProtocolKey = keyof TransportProtocol;
export type TransportProtocolValue = ValueOf<TransportProtocol>;
export const ReverseTransportProtocol = reverseRecord(TransportProtocol);


export const isClusterNodeRole = (n: number): n is ClusterNodeRoleValue =>
  n in ReverseClusterNodeRole;

export const isClusterNodeStatus = (n: number): n is ClusterNodeStatusValue =>
  n in ReverseClusterNodeStatus;

export const isTransportProtocol = (n: number): n is TransportProtocolValue =>
  n in ReverseTransportProtocol;


export const mapClusterNodeRole = (n: number): ClusterNodeRoleKey => {
  if(!isClusterNodeRole(n))
    throw new Error('Invalid ClusterNodeRole', { cause: { role: n, ClusterNodeRole }});
  return ReverseClusterNodeRole[n];
}

export const mapClusterNodeStatus = (n: number): ClusterNodeStatusKey => {
  if(!isClusterNodeStatus(n))
    throw new Error('Invalid ClusterNodeStatus', { cause: { status: n, ClusterNodeStatus }});
  return ReverseClusterNodeStatus[n];
}

export const mapTransportProtocol = (n: number): TransportProtocolKey => {
  if(!isTransportProtocol(n))
    throw new Error(
      'Invalid TransportProtocol', { cause: { transport: n, TransportProtocol } }
    );
  return ReverseTransportProtocol[n];
}
