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

import { env } from '$env/dynamic/public';
import { authStore } from '$lib/auth/authStore.svelte';
import type { ApiSchema } from './ApiSchema';
import { convertBigIntsToStrings } from './convertBigIntsToStrings';
import { getJson } from './getJson';

export const fetchRouteApi = async (
  arg: ApiSchema
): Promise<{ data: any; status: number; ok: boolean }> => {
  try {
    const { path, method, queryParams } = arg;

    const headers = new Headers();
    headers.set('Content-Type', 'application/json');

    const token = authStore.getAccessToken();
    if (token) {
      headers.set('Authorization', `Bearer ${token}`);
    }

    // Use PUBLIC_IGGY_API_URL if set, otherwise use relative path (for embedded mode)
    const baseUrl = env.PUBLIC_IGGY_API_URL || '';
    let fullUrl = `${baseUrl}${path}`;

    if (queryParams) {
      const params = Object.entries(queryParams).map(([k, v]) => [k, String(v)]);
      const query = new URLSearchParams(params);
      fullUrl += '?' + query.toString();
    }

    const res = await fetch(fullUrl, {
      headers,
      method,
      ...('body' in arg && arg.body ? { body: JSON.stringify(arg.body) } : {})
    });

    const data = await getJson(res);
    const safeData = convertBigIntsToStrings(data);

    return { data: safeData, status: res.status, ok: res.ok };
  } catch (err) {
    throw new Error('fetchRouteApi error');
  }
};
