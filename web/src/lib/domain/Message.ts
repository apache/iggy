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

import { formatDateWithMicroseconds } from '$lib/utils/formatters/dateFormatter';

export type MessagePartition = {
  partitionId: number;
  currentOffset: number;
  messages: Message[];
};

export type Message = {
  checksum: number;
  id: number;
  offset: number;
  timestamp: number;
  origin_timestamp: number;
  user_headers_length: number;
  payload_length: number;
  formattedTimestamp: string;
  user_headers: Record<string, HeaderField>;
  payload: string;
  truncatedPayload: string;
};

export type HeaderField = {
  kind: string;
  value: string;
};

export function messageMapper(item: any): Message {
  const payload = item.payload;
  const truncatedPayload = payload.length > 30 ? `${payload.slice(0, 30)} [...]` : payload;
  const formattedTimestamp = formatDateWithMicroseconds(item.header.timestamp);

  return {
    checksum: item.header.checksum,
    id: item.header.id,
    offset: item.header.offset,
    timestamp: item.header.timestamp,
    origin_timestamp: item.header.origin_timestamp,
    user_headers_length: item.header.user_headers_length,
    payload_length: item.header.payload_length,
    formattedTimestamp: formattedTimestamp,
    user_headers: item.user_headers,
    payload,
    truncatedPayload
  };
}

export function messagePartitionMapper(item: any): MessagePartition {
  return {
    partitionId: item.partition_id,
    currentOffset: item.current_offset,
    messages: item.messages.map(messageMapper)
  };
}
