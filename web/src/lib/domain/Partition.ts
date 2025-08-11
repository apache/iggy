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


import { formatDate } from '$lib/utils/formatters/dateFormatter';

export type Partition = {
  id: number;
  segmentsCount: number;
  currentOffset: number;
  sizeBytes: number;
  sizeFormatted: string;
  messagesCount: number;
  createdAt: string;
};

export function partitionMapper(item: any): Partition {
  return {
    id: item.id,
    segmentsCount: item.segments_count,
    currentOffset: item.current_offset,
    sizeBytes: item.size,
    sizeFormatted: item.size,
    messagesCount: item.messages_count,
    createdAt: formatDate(item.created_at)
  };
}
