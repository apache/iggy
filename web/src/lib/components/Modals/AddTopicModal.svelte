<script lang="ts">
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod } from 'sveltekit-superforms/adapters';
  import { z } from 'zod';
  import Input from '../Input.svelte';
  import Select from '../Select.svelte';
  import Button from '../Button.svelte';
  import ModalBase from './ModalBase.svelte';
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import type { StreamDetails } from '$lib/domain/StreamDetails';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { durationFormatter } from '$lib/utils/formatters/durationFormatter';
  import { numberSizes } from '$lib/utils/constants/numberSizes';
  import { dataHas } from '$lib/utils/dataHas';
  import { showToast } from '../AppToasts.svelte';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
    streamDetails: StreamDetails;
    nextTopicId: number;
  }

  let { closeModal, streamDetails, nextTopicId }: Props = $props();

  const schema = z.object({
    topic_id: z.number().min(0).max(numberSizes.max.u32).default(nextTopicId),
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters'),
    partitions_count: z.number().min(0).max(numberSizes.max.u32).default(1),
    message_expiry: z.number().min(0).max(numberSizes.max.u32).default(0),
    compression_algorithm: z.enum(["none", "gzip"]).default("none"),
    max_topic_size: z.number().min(0).max(numberSizes.max.u32).default(1_000_000_000),
  });

  const { form, errors, enhance, constraints } = superForm(defaults(zod(schema)), {
    SPA: true,
    validators: zod(schema),

    async onUpdate({ form }) {
      if (!form.valid) return;

      const { data, ok } = await fetchRouteApi({
        method: 'POST',
        path: `/streams/${streamDetails.id}/topics`,
        body: {
          stream_id: streamDetails.id,
          topic_id: form.data.topic_id,
          name: form.data.name,
          partitions_count: form.data.partitions_count,
          message_expiry: form.data.message_expiry,
          compression_algorithm: form.data.compression_algorithm,
          max_topic_size: form.data.max_topic_size,
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
            description: `Topic ${form.data.name} has been added.`,
            duration: 3500
          });
        });
      }
    }
  });
</script>

<ModalBase {closeModal} title="Add topic">
  <form method="POST" class="flex flex-col h-[650px] gap-4" use:enhance>
    <Input
      label="Id"
      name="topic_id"
      type="number"
      bind:value={$form.topic_id}
      {...$constraints.topic_id}
      errorMessage={$errors.topic_id?.join(',')}
    />
    <Input
      name="topicName"
      label="Name"
      bind:value={$form.name}
      errorMessage={$errors.name?.join(',')}
    />

    <Input
      label="Partitions count"
      type="number"
      name="partitionsCount"
      bind:value={$form.partitions_count}
      {...$constraints.partitions_count}
      errorMessage={$errors.partitions_count?.join(',')}
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

    <Select
      label="Compression Algorithm"
      type="text"
      name="compressionAlgorithm"
      options={["none", "gzip"]}
      bind:value={$form.compression_algorithm}
      {...$constraints.compression_algorithm}
      errorMessage={$errors.compression_algorithm?.join(',')}
    />

    <Input
      label="Max topic size"
      type="number"
      name="maxTopicSize"
      bind:value={$form.max_topic_size}
      {...$constraints.max_topic_size}
      errorMessage={$errors.max_topic_size?.join(',')}
    />

    <div class="flex justify-end gap-3 mt-auto">
      <Button variant="text" type="button" class="w-2/5" onclick={() => closeModal()}
        >Cancel</Button
      >
      <Button type="submit" variant="contained" class="w-2/5">Create</Button>
    </div>
  </form>
</ModalBase>
