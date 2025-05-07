import { fetchIggyApi } from '$lib/api/fetchApi';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { topicDetailsMapper } from '$lib/domain/TopicDetails';

export const load = async ({ params, cookies }) => {
  const getTopic = async () => {
    const result = await fetchIggyApi({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}`,
      cookies
    });

    const { data } = await handleFetchErrors(result, cookies);

    return topicDetailsMapper(data);
  };

  return {
    topic: await getTopic()
  };
};
