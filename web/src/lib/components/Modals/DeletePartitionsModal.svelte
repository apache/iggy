<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';

  import Input from '../Input.svelte';
  import { z } from 'zod';
  import ModalBase from './ModalBase.svelte';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod } from 'sveltekit-superforms/adapters';
  import Button from '../Button.svelte';

  import ModalConfirmation from '../ModalConfirmation.svelte';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { page } from '$app/state';
  import { showToast } from '../AppToasts.svelte';
  import { dataHas } from '$lib/utils/dataHas';
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

  const { form, errors, enhance, constraints, validateForm } = superForm(
    defaults(zod(schema)),
    {
      SPA: true,
      validators: zod(schema),

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

        if (dataHas(data, 'field', 'reason')) {
          return setError(form, data.field, data.reason);
        }

        if (ok) {
          closeModal(async () => {
            await customInvalidateAll();
            showToast({
              type: 'success',
              description: form.data.partitions_count > 1
                  ? `${form.data.partitions_count} partitions have been deleted.`
                  : '1 partition has been deleted.',
              duration: 3500
            });
          });
        }
      }
    }
  );

  let messagesToDelete = $derived(arraySum(
    topic.partitions.slice($form.partitions_count).map((p) => p.messagesCount)
  ));
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
        errorMessage={$errors.partitions_count?.join(',')}
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
