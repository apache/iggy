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
import { streamListMapper } from '$lib/domain/Stream';
import { streamDetailsMapper } from '$lib/domain/StreamDetails';
import { userMapper, type User } from '$lib/domain/User';
import type { PageLoad } from './$types';

export const load: PageLoad = async () => {
  const getUsers = async () => {
    const data = await clientApi<any[]>({
      method: 'GET',
      path: '/users'
    });

    return data.map((item: any) => userMapper(item)) as User[];
  };

  const getStreams = async () => {
    const data = await clientApi<any[]>({
      method: 'GET',
      path: '/streams'
    });

    const streams = streamListMapper(data);

    if (streams.length === 0) {
      return {
        streams,
        streamDetails: undefined
      };
    }

    const streamDetailsData = await clientApi({
      method: 'GET',
      path: `/streams/${streams[0].id}`
    });

    return {
      streams,
      streamDetails: streamDetailsMapper(streamDetailsData)
    };
  };

  const [users, streamsData] = await Promise.all([getUsers(), getStreams()]);

  return {
    users,
    streams: streamsData.streams,
    streamDetails: streamsData.streamDetails
  };
};
