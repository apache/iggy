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
import { error, json, type RequestHandler } from '@sveltejs/kit';
import { convertBigIntsToStrings } from '$lib/api/convertBigIntsToStrings';

export const POST: RequestHandler = async ({ request, cookies }) => {
  const { path, body, method, queryParams } = await request.json();

  if (!path || !method) {
    const message = `routes/api/proxy/+server.ts no path or body or method provided`;
    console.error(message);
    error(500, {
            message
          });
  }

  const result = await fetchIggyApi({ body, path, method, cookies, queryParams });

  const { data, response } = await handleFetchErrors(result, cookies);

  const safeData = convertBigIntsToStrings(data);

  return json({ data: safeData, ok: response.ok, status: response.status });
};
