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
import { checkIfPathnameIsPublic, typedRoute } from '$lib/types/appRoutes';
import { tokens } from '$lib/utils/constants/tokens';
import type { Handle } from '@sveltejs/kit';
import { sequence } from '@sveltejs/kit/hooks';
import { redirect } from '@sveltejs/kit';

console.log(`Iggy API URL: ${env.PUBLIC_IGGY_API_URL}`);

const accessTokenOkRedirects = [
  {
    from: '/dashboard/',
    to: typedRoute('/dashboard/overview')
  },
  {
    from: '/dashboard',
    to: typedRoute('/dashboard/overview')
  },
  {
    from: '/',
    to: typedRoute('/dashboard/overview')
  },
  {
    from: '/auth',
    to: typedRoute('/dashboard/overview')
  },
  {
    from: typedRoute('/auth/sign-in'),
    to: typedRoute('/dashboard/overview')
  }
];

const handleAuth: Handle = async ({ event, resolve }) => {
  const cookies = event.cookies;
  const isPublicPath = checkIfPathnameIsPublic(event.url.pathname);
  const accessToken = cookies.get(tokens.accessToken);

  if (!accessToken) {
    if (isPublicPath) {
      return resolve(event);
    } else {
      redirect(302, typedRoute('/auth/sign-in'));
    }
  }

  const invalidPathRedirect = accessTokenOkRedirects.find((r) => r.from === event.url.pathname);
  if (invalidPathRedirect) {
    redirect(302, invalidPathRedirect.to);
  }

  return resolve(event);
};

export const handle = sequence(handleAuth);
