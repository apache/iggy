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

import { browser } from '$app/environment';
import { goto } from '$app/navigation';
import { resolve } from '$app/paths';
import { clientApi } from '$lib/api/clientApi';
import { authStore } from '$lib/auth/authStore.svelte';
import { userDetailsMapper } from '$lib/domain/UserDetails';
import { typedRoute } from '$lib/types/appRoutes';
import { jwtDecode } from 'jwt-decode';
import type { LayoutLoad } from './$types';

export const load: LayoutLoad = async () => {
  if (browser) {
    authStore.initialize();
  }

  const token = authStore.getAccessToken();
  if (browser && !token) {
    goto(resolve(typedRoute('/auth/sign-in')));
    return { user: null };
  }

  const getDetailedUser = async () => {
    let userId = authStore.userId;

    if (!userId && token) {
      try {
        const decoded = jwtDecode(token);
        userId = decoded.sub ? parseInt(decoded.sub, 10) : null;
      } catch {
        return null;
      }
    }

    if (!userId) return null;

    try {
      const data = await clientApi({ method: 'GET', path: `/users/${userId}` });
      return userDetailsMapper(data);
    } catch {
      return null;
    }
  };

  return {
    user: await getDetailedUser()
  };
};
