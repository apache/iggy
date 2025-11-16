<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import { z } from 'zod';
  import Input from '../Input.svelte';
  import ModalBase from './ModalBase.svelte';
  import { numberSizes } from '$lib/utils/constants/numberSizes';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { page } from '$app/state';
  import { showToast } from '../AppToasts.svelte';
  import Button from '../Button.svelte';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
  }

  let { closeModal }: Props = $props();

  const schema = z.object({
    partitions_count: z.number().min(1).max(numberSizes.max.u32).default(1)
  });

  const { form, errors, enhance, constraints } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),
    invalidateAll: false,
    taintedMessage: false,
    async onUpdate({ form }) {
      if (!form.valid) return;
      if (!page.params.streamId || !page.params.topicId) return;

      const { data, ok } = await fetchRouteApi({
        method: 'POST',
        path: `/streams/${+page.params.streamId}/topics/${+page.params.topicId}/partitions`,
        body: {
          partitions_count: form.data.partitions_count
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
          showToast({
            type: 'success',
            description:
              form.data.partitions_count > 1
                ? `${form.data.partitions_count} partitions have been added.`
                : '1 partition has been added.',
            duration: 3500
          });
        });
      } else {
        // Handle API errors that don't have field-specific errors
        const errorMessage =
          typeof data === 'string' ? data : data?.message || 'Failed to add partitions';
        showToast({
          type: 'error',
          description: errorMessage,
          duration: 5000
        });
      }
    }
  });
</script>

<ModalBase {closeModal} title="Add partitions">
  <form method="POST" class="flex flex-col gap-4 h-[300px]" use:enhance>
    <Input
      label="Partitions count"
      type="number"
      name="partitionsCount"
      bind:value={$form.partitions_count}
      {...$constraints.partitions_count}
      errorMessage={$errors.partitions_count?.[0]}
    />

    <div class="flex justify-end gap-3 mt-auto">
      <Button variant="text" type="button" class="w-2/5" onclick={() => closeModal()}>Cancel</Button
      >
      <Button type="submit" variant="contained" class="w-2/5">Create</Button>
    </div>
  </form>
</ModalBase>
