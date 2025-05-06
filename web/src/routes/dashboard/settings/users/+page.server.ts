import { fetchIggyApi } from '$lib/api/fetchApi';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { streamListMapper } from '$lib/domain/Stream';
import { streamDetailsMapper } from '$lib/domain/StreamDetails.js';

import { userMapper, type User } from '$lib/domain/User.js';

export const load = async ({ cookies }) => {
  const getUsers = async () => {
    const result = await fetchIggyApi({
      method: 'GET',
      path: '/users',
      cookies
    });

    const { data } = await handleFetchErrors(result, cookies);
    return (data as any).map((item: any) => userMapper(item)) as User[];
  };

  const getStreams = async () => {
    const result = await fetchIggyApi({
      method: 'GET',
      path: '/streams',
      cookies
    });

    const { data } = await handleFetchErrors(result, cookies);
    const streams = streamListMapper(data);

    if (streams.length === 0) {
      return {
        streams,
        streamDetails: undefined
      };
    }

    const streamDetailResult = await fetchIggyApi({
      method: 'GET',
      path: `/streams/${streams[0].id}`,
      cookies
    });
    const { data: streamDetailsData } = await handleFetchErrors(streamDetailResult, cookies);

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
