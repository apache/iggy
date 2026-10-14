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

import { DeserializeError } from '../error.utils.js';
import { serializeIdentifier, type Id } from '../identifier.utils.js';
import type { PartitionContext } from '../vsr/header.js';

export const PARTITION_ROUTE_SIZE = 25;
const ROUTE_OFFSET = { incarnation: 0, metadataOp: 8, view: 16, partitionsCount: 20, primaryReplica: 24 };

export type PartitionRoute = {
  context: PartitionContext,
  view: number,
  partitionsCount: number,
  primaryReplica: number
};

export const deserializePartitionRoute = (data: Buffer): PartitionRoute => {
  if (data.length !== PARTITION_ROUTE_SIZE)
    throw new DeserializeError(`partition route must contain exactly ${PARTITION_ROUTE_SIZE} bytes`);
  return {
    context: {
      incarnation: data.readBigUInt64LE(ROUTE_OFFSET.incarnation),
      ownerGeneration: 0n,
      metadataOp: data.readBigUInt64LE(ROUTE_OFFSET.metadataOp)
    },
    view: data.readUInt32LE(ROUTE_OFFSET.view),
    partitionsCount: data.readUInt32LE(ROUTE_OFFSET.partitionsCount),
    primaryReplica: data.readUInt8(ROUTE_OFFSET.primaryReplica)
  };
};

export const serializePartitionRoute = (streamId: Id, topicId: Id, partitionId: number): Buffer => {
  const partition = Buffer.alloc(4);
  partition.writeUInt32LE(partitionId);
  return Buffer.concat([serializeIdentifier(streamId), serializeIdentifier(topicId), partition]);
};
