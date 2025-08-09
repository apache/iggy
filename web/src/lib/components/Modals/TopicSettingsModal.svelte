<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import type { TopicDetails } from '$lib/domain/TopicDetails';
  import { z } from 'zod';

  import Button from '../Button.svelte';
  import Icon from '../Icon.svelte';
  import Input from '../Input.svelte';
  import ModalBase from './ModalBase.svelte';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod } from 'sveltekit-superforms/adapters';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { dataHas } from '$lib/utils/dataHas';
  import { goto } from '$app/navigation';
  import { showToast } from '../AppToasts.svelte';
  import ModalConfirmation from '../ModalConfirmation.svelte';
  import { browser } from '$app/environment';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';
  import { numberSizes } from '$lib/utils/constants/numberSizes';
  import { page } from '$app/state';
  import { durationFormatter } from '$lib/utils/formatters/durationFormatter';

  interface Props {
    topic: TopicDetails;
    closeModal: CloseModalFn;
    onDeleteRedirectPath: string;
  }

  let { topic, closeModal, onDeleteRedirectPath }: Props = $props();

  let confirmationOpen = $state(false);

  const schema = z.object({
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters')
      .default(topic.name),
    message_expiry: z.number().min(0).max(numberSizes.max.u32).default(topic.messageExpiry)
  });

  const { form, errors, enhance, constraints, submitting, tainted } = superForm(
    defaults(zod(schema)),
    {
      SPA: true,
      validators: zod(schema),
      invalidateAll: false,
      taintedMessage: false,
      async onUpdate({ form }) {
        if (!form.valid) return;
        if (!page.params.streamId) return;

        const { data, ok } = await fetchRouteApi({
          method: 'PUT',
          path: `/streams/${+page.params.streamId}/topics/${topic.id}`,
          body: {
            name: form.data.name,
            message_expiry: form.data.message_expiry,
            compression_algorithm: topic.compressionAlgorithm,
            max_topic_size: 0
          }
        });

        if (dataHas(data, 'field', 'reason')) {
          return setError(form, data.field, data.reason);
        }

        if (ok) {
          closeModal(async () => {
            await customInvalidateAll();
            showToast({
              type: 'success',
              description: `Stream ${form.data.name} has been updated.`,
              duration: 3500
            });
          });
        }
      }
    }
  );

  const onConfirmationResult = async (e: any) => {
    const result = e.detail as boolean;
    confirmationOpen = false;

    if (result) {
      const { ok } = await fetchRouteApi({
        method: 'DELETE',
        path: `/streams/${+(page.params.streamId || '')}/topics/${topic.id}`
      });

      if (ok) {
        closeModal(async () => {
          if (!browser) return;
          await goto(onDeleteRedirectPath);
          await customInvalidateAll();
          showToast({
            type: 'success',
            description: `Topic ${topic.name} has been deleted.`,
            duration: 3500
          });
        });
      }
    }
  };
</script>

<ModalBase {closeModal} title="Topic settings">
  <ModalConfirmation
    open={confirmationOpen}
    retypeText={topic.name}
    deleteButtonTitle="Delete Topic"
    on:result={onConfirmationResult}
  >
    {#snippet message()}

        Deleting the topic "<span class="font-semibold">{topic.name}</span>" will permenently remove
        all associated <span class="font-semibold">partitions ({topic.partitionsCount})</span> and
        <span class="font-semibold">messages ({topic.messagesCount})</span>.

      {/snippet}
  </ModalConfirmation>

  <div class="h-[400px] flex flex-col">
    <form method="POST" class="flex flex-col gap-4 flex-[3] pb-5" use:enhance>
      <Input
        name="name"
        label="Name"
        bind:value={$form.name}
        errorMessage={$errors.name?.join(',')}
      />

      <Input
        label="Message expiry"
        type="number"
        name="messageExpiry"
        bind:value={$form.message_expiry}
        {...$constraints.message_expiry}
        errorMessage={$errors.message_expiry?.join(',')}
      />

      <span class="-mt-1 text-xs text-shadeD200 dark:text-shadeL700">
        {#if !$form.message_expiry || $form.message_expiry > numberSizes.max.u32}
          {#if $form.message_expiry === 0}
            never
          {/if}
        {:else}
          {durationFormatter(+$form.message_expiry)}
        {/if}
      </span>

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
      <div class="h-[1px] border-b absolute -left-7 -right-7"></div>
      <h2 class="text-xl text-color font-semibold mb-7 mt-5">Delete topic</h2>

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
