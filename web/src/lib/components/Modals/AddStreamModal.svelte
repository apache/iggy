<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod } from 'sveltekit-superforms/adapters';
  import { z } from 'zod';
  import Button from '../Button.svelte';
  import Input from '../Input.svelte';
  import ModalBase from './ModalBase.svelte';
  import { showToast } from '../AppToasts.svelte';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { dataHas } from '$lib/utils/dataHas';
  import { numberSizes } from '$lib/utils/constants/numberSizes';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';

  interface Props {
    closeModal: CloseModalFn;
    nextStreamId: number;
  }

  let { closeModal, nextStreamId }: Props = $props();

  const schema = z.object({
    stream_id: z.number().min(0).max(numberSizes.max.u32).default(nextStreamId),
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters')
  });

  const { form, errors, enhance, constraints, submitting } = superForm(
    defaults(zod(schema)),
    {
      SPA: true,
      validators: zod(schema),
      invalidateAll: false,
      taintedMessage: false,
      async onUpdate({ form }) {
        if (!form.valid) return;

        const { data, ok } = await fetchRouteApi({
          method: 'POST',
          path: '/streams',
          body: {
            stream_id: form.data.stream_id,
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
              description: `Stream ${form.data.name} has been added.`,
              duration: 3500
            });
          });
        }
      }
    }
  );
</script>

<ModalBase {closeModal} title="Add new stream">
  <form method="POST" class="flex flex-col h-[300px] gap-4" use:enhance>
    <Input
      label="Id"
      name="stream_id"
      type="number"
      bind:value={$form.stream_id}
      {...$constraints.stream_id}
      errorMessage={$errors.stream_id?.join(',')}
    />
    <Input
      name="name"
      label="Name"
      bind:value={$form.name}
      errorMessage={$errors.name?.join(',')}
    />

    <div class="flex justify-end gap-3 mt-auto w-full">
      <Button type="button" variant="text" class="w-2/5" onclick={() => closeModal()}
        >Cancel</Button
      >
      <Button type="submit" variant="contained" class="w-2/5" disabled={$submitting}>Create</Button>
    </div>
  </form>
</ModalBase>
