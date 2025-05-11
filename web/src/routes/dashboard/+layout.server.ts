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
