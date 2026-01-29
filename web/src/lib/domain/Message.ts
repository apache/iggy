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
  checksum: string;
  id: string;
  offset: string;
  timestamp: string;
  origin_timestamp: string;
  user_headers_length: number;
  payload_length: number;
  formattedTimestamp: string;
  user_headers: HeaderEntry[];
  payload: string;
  truncatedPayload: string;
};

export type HeaderField = {
  kind: string;
  value: string;
};

export type HeaderEntry = {
  key: HeaderField;
  value: HeaderField;
};

export function messageMapper(item: any): Message {
  const payload = item.payload;
  const truncatedPayload = payload.length > 30 ? `${payload.slice(0, 30)} [...]` : payload;

  // Convert all header numbers to strings to preserve precision
  // getJson() returns numbers for values <= MAX_SAFE_INTEGER, strings for larger values
  const checksum = String(item.header.checksum);
  const id = String(item.header.id);
  const offset = String(item.header.offset);
  const timestamp = String(item.header.timestamp);
  const origin_timestamp = String(item.header.origin_timestamp);

  // Use numeric value for date formatting (getJson already converted to number if safe)
  const timestampNum =
    typeof item.header.timestamp === 'number'
      ? item.header.timestamp
      : Number(item.header.timestamp);
  const formattedTimestamp = formatDateWithMicroseconds(timestampNum);

  return {
    checksum,
    id,
    offset,
    timestamp,
    origin_timestamp,
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
