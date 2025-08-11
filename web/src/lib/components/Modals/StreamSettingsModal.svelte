<script lang="ts">
  import type { StreamDetails } from '$lib/domain/StreamDetails';
  import type { CloseModalFn } from '$lib/types/utilTypes';
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
  import { typedRoute } from '$lib/types/appRoutes';
  import { browser } from '$app/environment';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';
  import { arraySum } from '$lib/utils/arraySum';

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

  const { form, errors, enhance, submitting, tainted } = superForm(
    defaults(zod(schema)),
    {
      SPA: true,
      validators: zod(schema),
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
        path: `/streams/${stream.id}`
      });

      if (ok) {
        closeModal(async () => {
          if (!browser) return;
          await goto(typedRoute('/dashboard/streams'));
          await customInvalidateAll();
          showToast({
            type: 'success',
            description: `Stream ${stream.name} has been deleted.`,
            duration: 3500
          });
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
    <form method="POST" class="flex flex-col h-[300px] gap-4 flex-[3] pb-5" use:enhance>
      <Input
        name="name"
        label="Name"
        bind:value={$form.name}
        errorMessage={$errors.name?.join(',')}
      />

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
