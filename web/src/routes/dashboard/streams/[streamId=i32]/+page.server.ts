import { fetchIggyApi } from '$lib/api/fetchApi.js';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { streamDetailsMapper } from '$lib/domain/StreamDetails';

export const load = async ({ params, cookies }) => {
  const getStreamDetails = async () => {
    const result = await fetchIggyApi({
      method: 'GET',
      path: `/streams/${+params.streamId}`,
      cookies
    });

    const { data } = await handleFetchErrors(result, cookies);

    return streamDetailsMapper(data);
  };

  return {
    streamDetails: await getStreamDetails()
  };
};
