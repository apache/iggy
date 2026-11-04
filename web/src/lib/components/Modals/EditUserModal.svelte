<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<script lang="ts">
  import type { CloseModalFn } from '#lib/types/utilTypes.js';
  import type { User } from '#lib/domain/User.js';
  import { z } from 'zod';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import Button from '../Button.svelte';
  import Input from '../Input.svelte';
  import Listbox from '../Listbox.svelte';
  import ModalBase from './ModalBase.svelte';
  import { fetchRouteApi } from '#lib/api/fetchRouteApi.js';
  import { userUpdate } from '#lib/api/resourceOptions.js';
  import { showToast } from '../AppToasts.svelte';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
    user: User;
  }

  let { closeModal, user }: Props = $props();

  // userMapper pads the username and marks root with a display suffix.
  const username = (user.id === 0 ? user.username.replace(/\(root\)$/, '') : user.username).trim();

  const schema = z.object({
    username: z.string().min(1, 'Username is required').trim().default(username),
    status: z.enum(['active', 'inactive']).default(user.status)
  });

  const { form, errors, enhance, constraints, submitting, tainted } = superForm(
    defaults(zod4(schema)),
    {
      SPA: true,
      validators: zod4(schema),
      invalidateAll: false,
      taintedMessage: false,
      async onUpdate({ form }) {
        if (!form.valid) return;

        const body = userUpdate({ current: { username, status: user.status }, next: form.data });
        if (Object.keys(body).length === 0) return closeModal();

        const { data, ok } = await fetchRouteApi({
          method: 'PUT',
          path: `/users/${user.id}`,
          body
        });

        if (!ok) {
          // Handle API errors
          if (data?.field && data?.reason) {
            // Field-specific error - show in form
            return setError(form, data.field, data.reason);
          } else if (data?.reason) {
            // General error with reason - show toast
            let errorMessage = data.reason;
            if (data.code && data.id) {
              errorMessage += `\n${data.code} (${data.id})`;
            } else if (data.code) {
              errorMessage += `\n${data.code}`;
            }
            showToast({
              type: 'error',
              description: errorMessage,
              duration: 5000
            });
          } else {
            // Fallback error message
            showToast({
              type: 'error',
              description: 'Failed to update user',
              duration: 5000
            });
          }
          return;
        }

        closeModal(async () => {
          await customInvalidateAll();
          showToast({
            type: 'success',
            description: `User ${form.data.username} has been updated.`,
            duration: 3500
          });
        });
      }
    }
  );
</script>

<ModalBase {closeModal} title="Edit user">
  <form method="POST" class="flex flex-col h-[300px] gap-4" use:enhance>
    <Input
      label="Username"
      name="username"
      bind:value={$form.username}
      {...$constraints.username}
      errorMessage={$errors.username?.[0]}
    />

    <Listbox
      label="Status"
      options={[
        { name: 'Active', value: 'active' },
        { name: 'Inactive', value: 'inactive' }
      ]}
      selectedValue={$form.status}
      on:selectedValue={(e) => ($form.status = e.detail as 'active' | 'inactive')}
    />

    <div class="flex justify-end gap-3 mt-auto w-full">
      <Button type="button" variant="text" class="w-2/5" onclick={() => closeModal()}>Cancel</Button
      >
      <Button type="submit" variant="contained" class="w-2/5" disabled={$submitting || !$tainted}
        >Update</Button
      >
    </div>
  </form>
</ModalBase>
