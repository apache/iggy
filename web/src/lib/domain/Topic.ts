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

import { bytesFormatter } from '$lib/utils/formatters/bytesFormatter';
import { formatDate } from '$lib/utils/formatters/dateFormatter';
import { durationFormatter } from '$lib/utils/formatters/durationFormatter';

export type Topic = {
  id: number;
  name: string;
  sizeBytes: string;
  sizeFormatted: string;
  messagesCount: number;
  messageExpiry: number;
  messageExpiryFormatted: string;
  partitionsCount: number;
  createdAt: string;
  compressionAlgorithm: number;
  maxTopicSize: number;
};

export function topicMapper(item: any): Topic {
  const micros: number = item.message_expiry ?? 0;

  const messageExpiry = micros === 0 || micros >= Number.MAX_SAFE_INTEGER ? 0 : micros;
  return {
    id: item.id,
    name: item.name,
    sizeBytes: item.size,
    sizeFormatted: bytesFormatter(item.size),
    messageExpiry,
    messageExpiryFormatted: formatExpiry(messageExpiry),
    messagesCount: item.messages_count,
    partitionsCount: item.partitions_count,
    createdAt: formatDate(item.created_at),
    compressionAlgorithm: item.compression_algorithm ?? 0,
    maxTopicSize: item.max_topic_size
  };
}

function formatExpiry(micros: number): string {
  if (micros === 0) return 'never';
  return durationFormatter(micros);
}
