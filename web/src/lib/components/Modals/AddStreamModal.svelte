<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import { z } from 'zod';
  import Button from '../Button.svelte';
  import Input from '../Input.svelte';
  import ModalBase from './ModalBase.svelte';
  import { showToast } from '../AppToasts.svelte';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
  }

  let { closeModal }: Props = $props();

  const schema = z.object({
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters')
  });

  const { form, errors, enhance, submitting } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),
    invalidateAll: false,
    taintedMessage: false,
    async onUpdate({ form }) {
      if (!form.valid) return;

      const { data, ok } = await fetchRouteApi({
        method: 'POST',
        path: '/streams',
        body: {
          name: form.data.name
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
            description: 'Failed to create stream',
            duration: 5000
          });
        }
        return;
      }

      // Success
      closeModal(async () => {
        await customInvalidateAll();
        showToast({
          type: 'success',
          description: `Stream ${form.data.name} has been added.`,
          duration: 3500
        });
      });
    }
  });
</script>

<ModalBase {closeModal} title="Add new stream">
  <form method="POST" class="flex flex-col h-[300px] gap-4" use:enhance>
    <Input name="name" label="Name" bind:value={$form.name} errorMessage={$errors.name?.[0]} />

    <div class="flex justify-end gap-3 mt-auto w-full">
      <Button type="button" variant="text" class="w-2/5" onclick={() => closeModal()}>Cancel</Button
      >
      <Button type="submit" variant="contained" class="w-2/5" disabled={$submitting}>Create</Button>
    </div>
  </form>
</ModalBase>
