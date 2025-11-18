<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';

  import Input from '../Input.svelte';
  import { z } from 'zod';
  import ModalBase from './ModalBase.svelte';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import Button from '../Button.svelte';

  import ModalConfirmation from '../ModalConfirmation.svelte';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { page } from '$app/state';
  import { showToast } from '../AppToasts.svelte';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';
  import type { TopicDetails } from '$lib/domain/TopicDetails';
  import { arraySum } from '$lib/utils/arraySum';

  interface Props {
    closeModal: CloseModalFn;
    topic: TopicDetails;
  }

  let { closeModal, topic }: Props = $props();

  let confirmationOpen = $state(false);
  let formElement: HTMLFormElement | undefined = $state();

  const onConfirmationResult = (e: any) => {
    const result = e.detail as boolean;
    confirmationOpen = false;

    if (result) {
      formElement?.requestSubmit();
    }
  };

  const schema = z.object({
    partitions_count: z.coerce.number().min(1).max(topic.partitionsCount).default(1)
  });

  const { form, errors, enhance, constraints, validateForm } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),

    async onUpdate({ form }) {
      if (!form.valid) return;
      if (!page.params.streamId || !page.params.topicId) return;

      const { data, ok } = await fetchRouteApi({
        method: 'DELETE',
        path: `/streams/${+page.params.streamId}/topics/${+page.params.topicId}/partitions`,
        queryParams: {
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
                ? `${form.data.partitions_count} partitions have been deleted.`
                : '1 partition has been deleted.',
            duration: 3500
          });
        });
      } else {
        // Handle API errors that don't have field-specific errors
        const errorMessage =
          typeof data === 'string' ? data : data?.message || 'Failed to delete partitions';
        showToast({
          type: 'error',
          description: errorMessage,
          duration: 5000
        });
      }
    }
  });

  let messagesToDelete = $derived(
    arraySum(topic.partitions.slice($form.partitions_count).map((p) => p.messagesCount))
  );
</script>

<ModalBase {closeModal} title="Delete partitions">
  <ModalConfirmation
    open={confirmationOpen}
    retypeText="{topic.name}/partitions"
    deleteButtonTitle="Delete {$form.partitions_count > 1 ? 'partitions' : 'partition'}"
    on:result={onConfirmationResult}
  >
    {#snippet message()}
      Deleting the <span class="font-semibold">{$form.partitions_count}</span>
      {$form.partitions_count > 1 ? 'partitions' : 'partition'} from topic
      <span class="font-semibold">"{topic.name}"</span> will permenently remove all associated
      messages <span class="font-semibold">({messagesToDelete})</span>.
    {/snippet}
  </ModalConfirmation>
  <div class="h-[300px] flex flex-col">
    <form bind:this={formElement} method="POST" class="flex flex-col h-[300px] gap-4" use:enhance>
      <Input
        label="Partitions count"
        name="partitions_count"
        bind:value={$form.partitions_count}
        type="number"
        {...$constraints.partitions_count}
        errorMessage={$errors.partitions_count?.[0]}
      />

      <div class="flex justify-end gap-3 mt-auto">
        <Button type="button" variant="text" class="w-2/5" onclick={() => closeModal()}
          >Cancel</Button
        >
        <Button
          type="button"
          variant="containedRed"
          class="w-2/5"
          onclick={async () => {
            const result = await validateForm();
            if (result.valid) confirmationOpen = true;
          }}>Delete</Button
        >
      </div>
    </form>
  </div>
</ModalBase>
