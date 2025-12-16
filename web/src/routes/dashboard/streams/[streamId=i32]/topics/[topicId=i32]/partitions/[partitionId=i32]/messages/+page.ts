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

import { clientApi } from '$lib/api/clientApi';
import { partitionMessagesDetailsMapper } from '$lib/domain/MessageDetails';
import { topicDetailsMapper } from '$lib/domain/TopicDetails';
import type { PageLoad } from './$types';

const MESSAGES_PER_PAGE = 20;

export const load: PageLoad = async ({ params, url }) => {
  const direction = url.searchParams.get('direction') || 'desc';

  const getPartitionMessages = async () => {
    // First, get the initial offset to determine total messages
    const initialData = await clientApi<any>({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}/messages`,
      queryParams: {
        kind: 'offset',
        value: '0',
        count: '1',
        auto_commit: 'false',
        partition_id: params.partitionId
      }
    });

    const initialMessages = partitionMessagesDetailsMapper(initialData);
    const totalMessages = initialMessages.currentOffset + 1;
    const offset =
      url.searchParams.get('offset') ??
      (direction === 'desc' ? Math.max(0, totalMessages - MESSAGES_PER_PAGE).toString() : '0');

    const data = await clientApi<any>({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}/messages`,
      queryParams: {
        kind: 'offset',
        value: offset.toString(),
        count: MESSAGES_PER_PAGE.toString(),
        auto_commit: 'false',
        partition_id: params.partitionId
      }
    });

    return partitionMessagesDetailsMapper(data);
  };

  const getTopic = async () => {
    const data = await clientApi({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}`
    });

    return topicDetailsMapper(data);
  };

  const [partitionMessages, topic] = await Promise.all([getPartitionMessages(), getTopic()]);

  const offset = url.searchParams.get('offset') || '0';

  return {
    partitionMessages,
    topic,
    pagination: {
      offset: parseInt(offset.toString()),
      count: MESSAGES_PER_PAGE
    }
  };
};
