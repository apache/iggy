<script lang="ts">
  import type { StreamDetails } from '$lib/domain/StreamDetails';
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import { z } from 'zod';
  import Button from '../Button.svelte';
  import Icon from '../Icon.svelte';
  import Input from '../Input.svelte';
  import ModalBase from './ModalBase.svelte';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { goto } from '$app/navigation';
  import { showToast } from '../AppToasts.svelte';
  import ModalConfirmation from '../ModalConfirmation.svelte';
  import { typedRoute } from '$lib/types/appRoutes';
  import { browser } from '$app/environment';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';
  import { arraySum } from '$lib/utils/arraySum';
  import { resolve } from '$app/paths';

  interface Props {
    stream: StreamDetails;
    closeModal: CloseModalFn;
  }

  let { stream, closeModal }: Props = $props();

  let confirmationOpen = $state(false);

  const schema = z.object({
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters')
      .default(stream.name)
  });

  const { form, errors, enhance, submitting, tainted } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),
    invalidateAll: false,
    taintedMessage: false,
    async onUpdate({ form }) {
      if (!form.valid) return;

      const { data, ok } = await fetchRouteApi({
        method: 'PUT',
        path: `/streams/${stream.id}`,
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
            description: `Stream ${form.data.name} has been updated.`,
            duration: 3500
          });
        });
      } else {
        // Handle API errors that don't have field-specific errors
        const errorMessage =
          typeof data === 'string' ? data : data?.message || 'Failed to update stream';
        showToast({
          type: 'error',
          description: errorMessage,
          duration: 5000
        });
      }
    }
  });

  const onConfirmationResult = async (e: any) => {
    const result = e.detail as boolean;
    confirmationOpen = false;

    if (result) {
      const { ok } = await fetchRouteApi({
        method: 'DELETE',
        path: `/streams/${stream.id}`
      });

      if (ok) {
        closeModal(async () => {
          if (!browser) return;
          await goto(resolve(typedRoute('/dashboard/streams')));
          await customInvalidateAll();
          showToast({
            type: 'success',
            description: `Stream ${stream.name} has been deleted.`,
            duration: 3500
          });
        });
      } else {
        // Handle API errors for stream deletion
        const errorMessage =
          typeof data === 'string' ? data : data?.message || 'Failed to delete stream';
        showToast({
          type: 'error',
          description: errorMessage,
          duration: 5000
        });
      }
    }
  };
</script>

<ModalBase {closeModal} title="Stream settings">
  <ModalConfirmation
    open={confirmationOpen}
    retypeText={stream.name}
    deleteButtonTitle="Delete Stream"
    on:result={onConfirmationResult}
  >
    {#snippet message()}
      Deleting the stream "<span class="font-semibold">{stream.name}</span>" will permenently remove
      all associated
      <span class="font-semibold">topics ({stream.topicsCount})</span>,
      <span class="font-semibold"
        >partitions ({arraySum(stream.topics.map((t) => t.partitionsCount))})</span
      >
      and
      <span class="font-semibold">messages ({stream.messagesCount})</span>.
    {/snippet}
  </ModalConfirmation>

  <div class="h-[350px] flex flex-col">
    <form method="POST" class="flex flex-col h-[300px] gap-4 flex-3 pb-5" use:enhance>
      <Input name="name" label="Name" bind:value={$form.name} errorMessage={$errors.name?.[0]} />

      <div class="flex justify-end gap-3 w-full mt-auto">
        <Button type="button" variant="text" class="w-2/5" onclick={() => closeModal()}
          >Cancel</Button
        >

        <Button type="submit" variant="contained" class="w-2/5" disabled={$submitting || !$tainted}
          >Update</Button
        >
      </div>
    </form>

    <div class="relative w-full flex-1">
      <div class="h-px border-b absolute -left-7 -right-7"></div>
      <h2 class="text-xl text-color font-semibold mb-7 mt-5">Delete stream</h2>

      <form class="w-full">
        <div class="flex text-color text-sm justify-between items-center mt-3">
          <span>Make sure it's safe operation</span>

          <Button
            variant="containedRed"
            class="max-h-[36px]"
            onclick={() => (confirmationOpen = true)}
          >
            <Icon name="trash" class="w-[20px] -ml-1" />
            Delete</Button
          >
        </div>
      </form>
    </div>
  </div>
</ModalBase>
