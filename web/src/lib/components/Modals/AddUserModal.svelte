<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import Listbox from '../Listbox.svelte';
  import Input from '../Input.svelte';
  import { z } from 'zod';
  import ModalBase from './ModalBase.svelte';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import PasswordInput from '../PasswordInput.svelte';
  import Button from '../Button.svelte';
  import PermissionsManager from '../PermissionsManager.svelte';
  import type { Stream } from '$lib/domain/Stream';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { showToast } from '../AppToasts.svelte';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
    streams: Stream[];
  }

  let { closeModal, streams }: Props = $props();

  const schema = z.object({
    username: z.string().min(1, 'Username is required').trim(),
    password: z.string().min(4, { message: 'Password must contain at least 4 characters' }).trim(),
    status: z.enum(['active', 'inactive']).default('active'),
    permissions: z.any().nullable().default(null)
  });

  const { form, errors, enhance, constraints } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),
    taintedMessage: false,

    async onUpdate({ form }) {
      if (!form.valid) return;

      const { data, ok } = await fetchRouteApi({
        method: 'POST',
        path: '/users',
        body: {
          username: form.data.username,
          password: form.data.password,
          status: form.data.status,
          permissions: form.data.permissions
        }
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
            description: 'Operation failed',
            duration: 5000
          });
        }
        return;
      }

      // Success
      if (ok) {
        closeModal(async () => {
          await customInvalidateAll();
          await showToast({
            type: 'success',
            description: `User ${form.data.username} has been created.`,
            duration: 3500
          });
        });
      } else {
        // Handle API errors that don't have field-specific errors
        const errorMessage =
          typeof data === 'string' ? data : data?.message || 'Failed to create user';
        showToast({
          type: 'error',
          description: errorMessage,
          duration: 5000
        });
      }
    }
  });
</script>

<ModalBase {closeModal} title="Add User">
  <form method="POST" class="flex flex-col" use:enhance>
    <div class="grid grid-cols-3 gap-4 min-w-[800px]">
      <Input
        label="Username"
        name="username"
        bind:value={$form.username}
        {...$constraints.username}
        errorMessage={$errors.username?.[0]}
      />

      <PasswordInput
        label="Password"
        name="password"
        autocomplete="new-password"
        errorMessage={$errors.password?.[0]}
        bind:value={$form.password}
        {...$constraints.password}
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
    </div>

    <PermissionsManager {streams} bind:value={$form.permissions} />

    <div class="flex justify-end gap-3 mt-16 w-[350px] ml-auto">
      <Button variant="text" type="button" class="w-2/5" onclick={() => closeModal()}>
        Cancel
      </Button>
      <Button type="submit" variant="contained" class="w-2/5">Add</Button>
    </div>
  </form>
</ModalBase>
