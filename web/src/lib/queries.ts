import { statsMapper } from '$lib/domain/Stats';
import { streamDetailsMapper } from '$lib/domain/StreamDetails';
// import { apiClient } from '$lib/utils/apiClient';

import { streamMapper, type Stream } from './domain/Stream';
import { topicDetailsMapper } from './domain/TopicDetails';

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
