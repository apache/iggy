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
import type { ApiSchema } from '$lib/api/ApiSchema';
import { tokens } from '$lib/utils/constants/tokens';
import { type Cookies } from '@sveltejs/kit';

export async function fetchIggyApi(
  args: ApiSchema & { queryParams?: Record<string, string>; cookies?: Cookies }
): Promise<Response | unknown> {
  const { path, method, queryParams, cookies } = args;

  try {
    const headers = new Headers();
    headers.set('Content-Type', 'application/json');

    if (cookies) {
      const accessToken = cookies.get(tokens.accessToken);
      if (accessToken) headers.set('Authorization', `Bearer ${accessToken}`);
    }

    let fullUrl = `${env.PUBLIC_IGGY_API_URL}${path}`;

    if (queryParams) {
      const query = new URLSearchParams(Object.entries(queryParams));
      fullUrl += '?' + query.toString();
    }

    const result = await fetch(fullUrl, {
      headers,
      method,
      ...('body' in args ? { body: JSON.stringify(args.body) } : {})
    });

    return result;
  } catch (e: unknown) {
    return e;
  }
}
