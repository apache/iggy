// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

import { goto } from '$app/navigation';
import { resolve } from '$app/paths';
import { PUBLIC_IGGY_API_URL } from '$app/env/public';
import { authStore } from '#lib/auth/authStore.svelte.js';
import { typedRoute } from '#lib/types/appRoutes.js';
import { getJson } from './getJson';
import type { MetadataChangesPage } from './metadataChanges';

export type MetadataChangesResult =
  | { kind: 'page'; page: MetadataChangesPage }
  | { kind: 'forbidden' }
  | { kind: 'refused'; reason: string }
  | { kind: 'failed'; reason: string };

/**
 * One page of `GET /metadata/changes`. Unlike `clientApi`, a 403 keeps the
 * session: it means the account lacks `manage_servers`, not that it expired.
 */
export async function fetchMetadataChanges(
  query: Record<string, string>,
  signal?: AbortSignal
): Promise<MetadataChangesResult> {
  const headers = new Headers();
  const token = authStore.getAccessToken();
  if (token) {
    headers.set('Authorization', `Bearer ${token}`);
  }
  const url = `${PUBLIC_IGGY_API_URL || ''}/metadata/changes?${new URLSearchParams(Object.entries(query))}`;
  let response: Response;
  try {
    response = await fetch(url, { headers, signal });
  } catch {
    return { kind: 'failed', reason: 'The server did not answer' };
  }
  const data = (await getJson(response)) as { reason?: string } | null;
  if (response.ok) {
    return { kind: 'page', page: data as MetadataChangesPage };
  }
  switch (response.status) {
    case 401:
      authStore.logout();
      await goto(resolve(typedRoute('auth/sign-in')));
      return { kind: 'failed', reason: 'The session ended' };
    case 403:
      return { kind: 'forbidden' };
    case 400:
      return { kind: 'refused', reason: data?.reason ?? 'The server refused the filter' };
    default:
      return { kind: 'failed', reason: `The server answered ${response.status}` };
  }
}
