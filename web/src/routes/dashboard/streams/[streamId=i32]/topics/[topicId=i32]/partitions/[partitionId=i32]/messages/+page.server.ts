import { fetchIggyApi } from '$lib/api/fetchApi';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { partitionMessagesDetailsMapper } from '$lib/domain/MessageDetails';
import { topicDetailsMapper } from '$lib/domain/TopicDetails';

const MESSAGES_PER_PAGE = 20;

export const load = async ({ params, cookies, url }) => {
  const offset = url.searchParams.get('offset') || '0';
  const direction = url.searchParams.get('direction') || 'desc';

  const getPartitionMessages = async () => {
    const initialResult = await fetchIggyApi({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}/messages`,
      cookies,
      queryParams: {
        kind: 'offset',
        value: '0',
        count: '1',
        auto_commit: 'false',
        partition_id: params.partitionId
      }
    });

    const { data: initialData } = await handleFetchErrors(initialResult, cookies);
    const initialMessages = partitionMessagesDetailsMapper(initialData as any);

    const totalMessages = initialMessages.currentOffset;
    const offset =
      url.searchParams.get('offset') ??
      (direction === 'desc' ? Math.max(0, totalMessages - MESSAGES_PER_PAGE) : '0');

    const result = await fetchIggyApi({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}/messages`,
      cookies,
      queryParams: {
        kind: 'offset',
        value: offset.toString(),
        count: MESSAGES_PER_PAGE.toString(),
        auto_commit: 'false',
        partition_id: params.partitionId
      }
    });

    const { data } = await handleFetchErrors(result, cookies);
    return partitionMessagesDetailsMapper(data as any);
  };

  const getTopic = async () => {
    const result = await fetchIggyApi({
      method: 'GET',
      path: `/streams/${+params.streamId}/topics/${+params.topicId}`,
      cookies
    });

    const { data } = await handleFetchErrors(result, cookies);
    return topicDetailsMapper(data);
  };

  const [partitionMessages, topic] = await Promise.all([getPartitionMessages(), getTopic()]);

  return {
    partitionMessages,
    topic,
    pagination: {
      offset,
      count: MESSAGES_PER_PAGE
    }
  };
};
