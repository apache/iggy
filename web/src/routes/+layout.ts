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
import { base, resolve } from '$app/paths';
import { authStore } from '$lib/auth/authStore.svelte';
import { checkIfPathnameIsPublic, typedRoute } from '$lib/types/appRoutes';
import type { LayoutLoad } from './$types';

// Enable client-side rendering for SPA mode
export const ssr = false;
export const prerender = false;

export const load: LayoutLoad = async ({ url }) => {
  if (browser) {
    authStore.initialize();
  }

  const pathname = url.pathname;
  const isPublicPath = checkIfPathnameIsPublic(pathname);
  const isAuthenticated = authStore.getAccessToken() !== null;

  const authRedirects = [
    base,
    `${base}/`,
    `${base}/dashboard`,
    `${base}/dashboard/`,
    `${base}/auth`,
    `${base}/auth/sign-in`
  ];

  if (browser) {
    if (!isAuthenticated && !isPublicPath) {
      goto(resolve(typedRoute('/auth/sign-in')));
      return { isAuthenticated: false };
    }

    if (isAuthenticated && authRedirects.includes(pathname)) {
      goto(resolve(typedRoute('/dashboard/overview')));
      return { isAuthenticated: true };
    }
  }

  return {
    isAuthenticated
  };
};
