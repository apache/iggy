<script lang="ts">
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
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
  import { showToast } from '../AppToasts.svelte';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
    streamDetails: StreamDetails;
  }

  let { closeModal, streamDetails }: Props = $props();

  const schema = z.object({
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters'),
    partitions_count: z.number().min(0).max(numberSizes.max.u32).default(1),
    message_expiry: z.number().min(0).max(numberSizes.max.u32).default(0),
    compression_algorithm: z.enum(['none', 'gzip']).default('none'),
    max_topic_size: z.number().min(0).max(numberSizes.max.u32).default(2_000_000_000)
  });

  const { form, errors, enhance, constraints, submitting } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),

    async onUpdate({ form }) {
      if (!form.valid) return;

      const { data, ok } = await fetchRouteApi({
        method: 'POST',
        path: `/streams/${streamDetails.id}/topics`,
        body: {
          name: form.data.name,
          partitions_count: form.data.partitions_count,
          message_expiry: form.data.message_expiry,
          compression_algorithm: form.data.compression_algorithm,
          max_topic_size: form.data.max_topic_size
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
            description: `Topic ${form.data.name} has been added.`,
            duration: 3500
          });
        });
      } else {
        // Handle API errors that don't have field-specific errors
        const errorMessage =
          typeof data === 'string' ? data : data?.message || 'Failed to create topic';
        showToast({
          type: 'error',
          description: errorMessage,
          duration: 5000
        });
      }
    }
  });
</script>

<ModalBase {closeModal} title="Add topic">
  <form method="POST" class="flex flex-col h-[650px] gap-4" use:enhance>
    <Input name="topicName" label="Name" bind:value={$form.name} errorMessage={$errors.name?.[0]} />

    <Input
      label="Partitions count"
      type="number"
      name="partitionsCount"
      bind:value={$form.partitions_count}
      {...$constraints.partitions_count}
      errorMessage={$errors.partitions_count?.[0]}
    />

    <Input
      label="Message expiry"
      type="number"
      name="messageExpiry"
      bind:value={$form.message_expiry}
      {...$constraints.message_expiry}
      errorMessage={$errors.message_expiry?.[0]}
    />

    <span class="-mt-1 text-xs text-shade-d200 dark:text-shade-l700">
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
      options={['none', 'gzip']}
      bind:value={$form.compression_algorithm}
      {...$constraints.compression_algorithm}
      errorMessage={$errors.compression_algorithm?.[0]}
    />

    <Input
      label="Max topic size"
      type="number"
      name="maxTopicSize"
      bind:value={$form.max_topic_size}
      {...$constraints.max_topic_size}
      errorMessage={$errors.max_topic_size?.[0]}
    />

    <div class="flex justify-end gap-3 mt-auto">
      <Button variant="text" type="button" class="w-2/5" onclick={() => closeModal()}>Cancel</Button
      >
      <Button type="submit" variant="contained" class="w-2/5" disabled={$submitting}>Create</Button>
    </div>
  </form>
</ModalBase>
