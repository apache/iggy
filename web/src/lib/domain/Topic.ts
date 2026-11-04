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

import {
  UINT64_MAX,
  UNLIMITED,
  isOptionExplicit,
  optionText,
  optionUint64
} from '#lib/api/resourceOptions.js';
import { bytesFormatter } from '#lib/utils/formatters/bytesFormatter.js';
import { formatDate } from '#lib/utils/formatters/dateFormatter.js';
import { durationFormatter } from '#lib/utils/formatters/durationFormatter.js';

export type Topic = {
  id: number;
  name: string;
  sizeBytes: string;
  sizeFormatted: string;
  messagesCount: number;
  messageExpiry: number;
  messageExpiryFormatted: string;
  messageExpiryExplicit: boolean;
  partitionsCount: number;
  createdAt: string;
  compressionAlgorithm: string;
  maxTopicSize: string;
  maxTopicSizeExplicit: boolean;
};

export function topicMapper(item: any): Topic {
  const micros = optionUint64(item.options, 'message_expiry') ?? 0n;
  const maxTopicSize = optionUint64(item.options, 'max_topic_size') ?? UINT64_MAX;

  const messageExpiry =
    micros === 0n || micros >= BigInt(Number.MAX_SAFE_INTEGER) ? 0 : Number(micros);
  return {
    id: item.id,
    name: item.name,
    sizeBytes: item.size,
    sizeFormatted: bytesFormatter(item.size),
    messageExpiry,
    messageExpiryFormatted: formatExpiry(messageExpiry),
    messageExpiryExplicit: isOptionExplicit(item.options, 'message_expiry'),
    messagesCount: item.messages_count,
    partitionsCount: item.partitions_count,
    createdAt: formatDate(item.created_at),
    compressionAlgorithm: optionText(item.options, 'compression_algorithm') ?? 'none',
    maxTopicSize: maxTopicSize === UINT64_MAX ? UNLIMITED : maxTopicSize.toString(),
    maxTopicSizeExplicit: isOptionExplicit(item.options, 'max_topic_size')
  };
}

function formatExpiry(micros: number): string {
  if (micros === 0) return 'never';
  return durationFormatter(micros);
}
