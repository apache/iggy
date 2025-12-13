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
import { typedRoute } from '$lib/types/appRoutes';
import { tokens } from '$lib/utils/constants/tokens';
import { jwtDecode } from 'jwt-decode';

export interface AuthState {
  isAuthenticated: boolean;
  accessToken: string | null;
  userId: number | null;
}

function createAuthStore() {
  let state = $state<AuthState>({
    isAuthenticated: false,
    accessToken: null,
    userId: null
  });

  function getTokenFromCookie(): string | null {
    if (!browser) return null;
    const match = document.cookie.match(new RegExp(`(^| )${tokens.accessToken}=([^;]+)`));
    return match ? match[2] : null;
  }

  function setTokenCookie(token: string, expiry: number): void {
    if (!browser) return;
    const expires = new Date(expiry * 1000);
    document.cookie = `${tokens.accessToken}=${token}; path=/; expires=${expires.toUTCString()}; SameSite=Lax`;
  }

  function clearTokenCookie(): void {
    if (!browser) return;
    document.cookie = `${tokens.accessToken}=; path=/; expires=Thu, 01 Jan 1970 00:00:00 GMT`;
  }

  function initialize(): void {
    const token = getTokenFromCookie();
    if (token) {
      try {
        const decoded = jwtDecode(token);
        const userId = decoded.sub ? parseInt(decoded.sub, 10) : null;
        state = {
          isAuthenticated: true,
          accessToken: token,
          userId
        };
      } catch {
        clearTokenCookie();
        state = { isAuthenticated: false, accessToken: null, userId: null };
      }
    }
  }

  function login(token: string, expiry: number): void {
    setTokenCookie(token, expiry);
    try {
      const decoded = jwtDecode(token);
      const userId = decoded.sub ? parseInt(decoded.sub, 10) : null;
      state = {
        isAuthenticated: true,
        accessToken: token,
        userId
      };
    } catch {
      state = { isAuthenticated: true, accessToken: token, userId: null };
    }
  }

  function logout(): void {
    clearTokenCookie();
    state = { isAuthenticated: false, accessToken: null, userId: null };
    goto(resolve(typedRoute('/auth/sign-in')));
  }

  function getAccessToken(): string | null {
    return state.accessToken || getTokenFromCookie();
  }

  // Initialize on creation if in browser
  if (browser) {
    initialize();
  }

  return {
    get state() {
      return state;
    },
    get isAuthenticated() {
      return state.isAuthenticated;
    },
    get accessToken() {
      return state.accessToken;
    },
    get userId() {
      return state.userId;
    },
    initialize,
    login,
    logout,
    getAccessToken
  };
}

export const authStore = createAuthStore();
