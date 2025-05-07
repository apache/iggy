import { fetchIggyApi } from '$lib/api/fetchApi';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { streamMapper, type Stream } from '$lib/domain/Stream';
import type { LayoutServerLoad } from './$types';

export const load: LayoutServerLoad = async ({ cookies }) => {
  const result = await fetchIggyApi({
    method: 'GET',
    path: '/streams',
    cookies
  });

  const { data } = await handleFetchErrors(result, cookies);

  return {
    streams: ((data as any).map(streamMapper) as Stream[]).sort((a, b) => b.createdAt - a.createdAt)
  };
};
