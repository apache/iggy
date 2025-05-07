import { fetchIggyApi } from '$lib/api/fetchApi';
import { handleFetchErrors } from '$lib/api/handleFetchErrors';
import { userDetailsMapper } from '$lib/domain/UserDetails';
import type { LayoutServerLoad } from './$types';
import { jwtDecode } from 'jwt-decode';

export const load: LayoutServerLoad = async ({ cookies }) => {
  const getDetailedUser = async () => {
    //always available here, auth hook prevents rendering this page without access_token
    const accessToken = cookies.get('access_token')!;
    const userId = jwtDecode(accessToken).sub!;

    const userResult = await fetchIggyApi({ method: 'GET', path: `/users/${+userId}`, cookies });
    const { data } = await handleFetchErrors(userResult, cookies);

    return userDetailsMapper(data);
  };

  return {
    user: await getDetailedUser()
  };
};
