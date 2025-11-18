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

import { fail, redirect } from '@sveltejs/kit';
import type { Actions } from './$types';
import { message, superValidate } from 'sveltekit-superforms/server';
import { zod4 } from 'sveltekit-superforms/adapters';
import { z } from 'zod';
import { typedRoute } from '$lib/types/appRoutes';
import { fetchIggyApi } from '$lib/api/fetchApi';
import { tokens } from '$lib/utils/constants/tokens';
import { getJson } from '$lib/api/getJson';

const schema = z.object({
  username: z.string().min(1),
  password: z.string().min(4)
});

type FormSchema = z.infer<typeof schema>;

export const load = async () => {
  const form = await superValidate(zod4(schema));

  return { form };
};

export const actions = {
  default: async ({ request, cookies }) => {
    const form = await superValidate(request, zod4(schema));

    if (!form.valid) {
      return fail(400, { form });
    }

    const { password, username } = form.data;

    const result = await fetchIggyApi({
      method: 'POST',
      path: '/users/login',
      body: { username, password }
    });

    console.log(result);

    if (!(result instanceof Response) || !result.ok) {
      return message(form, 'Username or password is not valid', { status: 403 });
    }

    const { access_token } = (await getJson(result)) as any;

    cookies.set(tokens.accessToken, access_token.token, {
      path: '/',
      httpOnly: true,
      sameSite: 'lax',
      secure: true,
      expires: new Date(1000 * access_token.expiry)
    });

    redirect(302, typedRoute('/dashboard/overview'));
  }
} satisfies Actions;
