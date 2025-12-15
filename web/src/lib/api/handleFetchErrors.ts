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

import { error, type Cookies, redirect } from '@sveltejs/kit';
import { base } from '$app/paths';
import { getJson } from './getJson';
import { tokens } from '$lib/utils/constants/tokens';
import { typedRoute } from '$lib/types/appRoutes';

export const handleFetchErrors = async (
  fetchResult: Response | unknown,
  cookies: Cookies
): Promise<{ response: Response; data: unknown }> => {
  if (!(fetchResult instanceof Response)) {
    error(500, 'handleFetchErrors: noResponse');
  }

  const response = fetchResult;
  const data = await getJson(response);

  if (response.ok) {
    return {
      response,
      data
    };
  }

  const removeCookies = () => {
    cookies.set(tokens.accessToken, '', {
      path: '/',
      expires: new Date(0)
    });
  };

  const errorHandlers = {
    400: () => {
      console.log(`handleErrorStatus: 400 ${response.url}`);

      return {
        response,
        data
      };
    },
    401: () => {
      console.log(`handleErrorStatus: 401 ${response.url}`);
      removeCookies();
      redirect(302, `${base}${typedRoute('/auth/sign-in')}`);
    },
    403: () => {
      console.log(`handleErrorStatus: 403 ${response.url}`);
      removeCookies();
      redirect(302, `${base}${typedRoute('/auth/sign-in')}`);
    },
    404: () => {
      console.log(`handleErrorStatus: 404 ${response.url}`);
      error(404, {
        message: 'Not Found'
      });
    }
  };

  const handler = (errorHandlers as any)[response.status];
  if (handler) return handler();

  console.log(`handleErrorStatus: 500 ${response.url}`);

  error(500, {
    message: 'handleErrorStatus: Internal server error'
  });
};
