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
import { env } from '$env/dynamic/public';
import { authStore } from '$lib/auth/authStore.svelte';
import { typedRoute } from '$lib/types/appRoutes';
import { error } from '@sveltejs/kit';
import { getJson } from './getJson';

export interface ApiRequest {
  path: string;
  method: 'GET' | 'POST' | 'PUT' | 'DELETE' | 'PATCH';
  body?: unknown;
  queryParams?: Record<string, string>;
}

export async function clientApi<T = unknown>(args: ApiRequest): Promise<T> {
  const { path, method, queryParams, body } = args;

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
    const query = new URLSearchParams(Object.entries(queryParams));
    fullUrl += '?' + query.toString();
  }

  const response = await fetch(fullUrl, {
    headers,
    method,
    ...(body ? { body: JSON.stringify(body) } : {})
  });

  const data = await getJson(response);

  if (response.ok) {
    return data as T;
  }

  // Handle errors
  if (response.status === 401 || response.status === 403) {
    if (browser) {
      authStore.logout();
    }
    error(401, { message: 'Unauthorized' });
  }

  if (response.status === 404) {
    error(404, { message: 'Not Found' });
  }

  if (response.status === 400) {
    // Return the error data for form validation
    throw { status: 400, data };
  }

  error(500, { message: 'Internal server error' });
}

/**
 * Wrapper that handles errors gracefully for load functions
 */
export async function clientApiSafe<T = unknown>(
  args: ApiRequest
): Promise<{ data: T | null; error: string | null }> {
  try {
    const data = await clientApi<T>(args);
    return { data, error: null };
  } catch (e: any) {
    if (e?.status === 401 || e?.status === 403) {
      if (browser) {
        goto(resolve(typedRoute('/auth/sign-in')));
      }
      return { data: null, error: 'Unauthorized' };
    }
    return { data: null, error: e?.message || 'Unknown error' };
  }
}
