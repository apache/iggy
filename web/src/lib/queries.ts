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

// import { apiClient } from '$lib/utils/apiClient';

const apiClient = async () => {};

// export function getStatsQuery() {
//   return createQuery({
//     queryKey: ['stats'],
//     refetchOnWindowFocus: false,
//     refetchOnMount: false,

//     queryFn: async () => {
//       const result:any; = await apiClient.get({ path: '/stats', throwOnError: true });
//       return statsMapper((result as any).data);
//     }
//   });
// }

// export function getStreamDetailsQuery(id: number) {
//   return createQuery({
//     queryKey: ['streamDetails', id],
//     refetchOnWindowFocus: false,
//     refetchOnMount: false,
//     queryFn: async () => {
//       const result = await apiClient.get({ path: `/streams/${id}`, throwOnError: true });

//       return streamDetailsMapper((result as any).data);
//     }
//   });
// }

// export function getStreamsQuery() {
//   return createQuery({
//     queryKey: ['streams'],
//     queryFn: async () => {
//       const res = await apiClient.get({ path: '/streams', throwOnError: true });
//       return (res as any).data.map(streamMapper) as Stream[];
//     }
//   });
// }

// export function getTopicDetailsQuery(streamId: number, topicId: number) {
//   return createQuery({
//     queryKey: ['topicDetails', streamId, topicId],
//     queryFn: async () => {
//       const result = await apiClient.get({
//         path: `/streams/${streamId}/topics/${topicId}`,
//         throwOnError: true
//       });

//       return topicDetailsMapper((result as any).data);
//     }
//   });
// }
