<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<script lang="ts">
  import type { CloseModalFn } from '#lib/types/utilTypes.js';
  import type { TopicDetails } from '#lib/domain/TopicDetails.js';
  import { z } from 'zod';

  import Button from '../Button.svelte';
  import Checkbox from '../Checkbox.svelte';
  import Icon from '../Icon.svelte';
  import Input from '../Input.svelte';
  import ModalBase from './ModalBase.svelte';
  import { setError, superForm, defaults } from 'sveltekit-superforms/client';
  import { zod4 } from 'sveltekit-superforms/adapters';
  import { fetchRouteApi } from '#lib/api/fetchRouteApi.js';
  import { topicUpdate, type TopicResettableKey } from '#lib/api/resourceOptions.js';
  import { goto } from '$app/navigation';
  import { resolve } from '$app/paths';
  import type { Path } from '$app/types';
  import { showToast } from '../AppToasts.svelte';
  import ModalConfirmation from '../ModalConfirmation.svelte';
  import { browser } from '$app/env';
  import { customInvalidateAll } from '../PeriodicInvalidator.svelte';
  import { page } from '$app/state';
  import DurationInput from '../DurationInput.svelte';

  interface Props {
    topic: TopicDetails;
    closeModal: CloseModalFn;
    onDeleteRedirectPath: Path;
  }

  let { topic, closeModal, onDeleteRedirectPath }: Props = $props();

  let confirmationOpen = $state(false);
  let resetMessageExpiry = $state(false);
  let resetMaxTopicSize = $state(false);

  const schema = z.object({
    name: z
      .string()
      .min(1, 'Name must contain at least 1 character')
      .max(255, 'Name must not exceed 255 characters')
      .default(topic.name),
    message_expiry: z.number().min(0).max(Number.MAX_SAFE_INTEGER).default(topic.messageExpiry),
    max_topic_size: z
      .string()
      .trim()
      .min(1, 'Max topic size is required')
      .default(topic.maxTopicSize)
  });

  const { form, errors, enhance, submitting, tainted } = superForm(defaults(zod4(schema)), {
    SPA: true,
    validators: zod4(schema),
    invalidateAll: false,
    taintedMessage: false,
    async onUpdate({ form }) {
      if (!form.valid) return;
      if (!page.params.streamId) return;

      const reset: TopicResettableKey[] = [];
      if (resetMessageExpiry) reset.push('message_expiry');
      if (resetMaxTopicSize) reset.push('max_topic_size');

      const body = topicUpdate({
        current: topic,
        next: {
          name: form.data.name,
          messageExpiry: form.data.message_expiry,
          maxTopicSize: form.data.max_topic_size
        },
        reset
      });
      if (Object.keys(body).length === 0) return closeModal();

      const { data, ok } = await fetchRouteApi({
        method: 'PUT',
        path: `/streams/${+page.params.streamId}/topics/${topic.id}`,
        body
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
            description: `Topic ${form.data.name} has been updated.`,
            duration: 3500
          });
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
        path: `/streams/${+(page.params.streamId || '')}/topics/${topic.id}`
      });

      if (ok) {
        closeModal(async () => {
          if (!browser) return;
          await goto(resolve(onDeleteRedirectPath));
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
      Deleting the topic "<span class="font-semibold">{topic.name}</span>" will permanently remove
      all associated <span class="font-semibold">partitions ({topic.partitionsCount})</span> and
      <span class="font-semibold">messages ({topic.messagesCount})</span>.
    {/snippet}
  </ModalConfirmation>

  <div class="min-h-[400px] flex flex-col">
    <form method="POST" class="flex flex-col gap-4 flex-3 pb-5" use:enhance>
      <Input name="name" label="Name" bind:value={$form.name} errorMessage={$errors.name?.[0]} />

      {#if resetMessageExpiry}
        <div class="flex flex-col gap-2">
          <span class="text-sm text-color ml-1">Message expiry</span>
          <span class="text-xs text-shade-d200 dark:text-shade-l700 ml-1">
            Returns to the server default on update.
          </span>
        </div>
      {:else}
        <DurationInput label="Message expiry" bind:value={$form.message_expiry} />
      {/if}

      {#if topic.messageExpiryExplicit}
        <div class="flex items-center gap-2">
          <Checkbox bind:checked={resetMessageExpiry} value="message_expiry" />
          <span class="text-sm text-color select-none">Reset message expiry to server default</span>
        </div>
      {/if}

      {#if resetMaxTopicSize}
        <div class="flex flex-col gap-2">
          <span class="text-sm text-color ml-1">Max topic size</span>
          <span class="text-xs text-shade-d200 dark:text-shade-l700 ml-1">
            Returns to the server default on update.
          </span>
        </div>
      {:else}
        <div class="flex flex-col gap-2">
          <Input
            name="maxTopicSize"
            label="Max topic size"
            placeholder="e.g. 1 GiB or unlimited"
            bind:value={$form.max_topic_size}
            errorMessage={$errors.max_topic_size?.[0]}
          />
          <span class="text-xs text-shade-d200 dark:text-shade-l700 ml-1">
            Bytes, a size such as 1 GiB, or unlimited.
          </span>
        </div>
      {/if}

      {#if topic.maxTopicSizeExplicit}
        <div class="flex items-center gap-2">
          <Checkbox bind:checked={resetMaxTopicSize} value="max_topic_size" />
          <span class="text-sm text-color select-none">Reset max topic size to server default</span>
        </div>
      {/if}

      <div class="flex flex-col gap-2">
        <Input
          name="compressionAlgorithm"
          label="Compression algorithm"
          value={topic.compressionAlgorithm}
          readonly
        />
        <span class="text-xs text-shade-d200 dark:text-shade-l700 ml-1">
          Set when the topic is created and cannot be changed.
        </span>
      </div>

      <div class="flex justify-end gap-3 w-full mt-auto">
        <Button type="button" variant="text" class="w-2/5" onclick={() => closeModal()}
          >Cancel</Button
        >

        <Button
          type="submit"
          variant="contained"
          class="w-2/5"
          disabled={$submitting || (!$tainted && !resetMessageExpiry && !resetMaxTopicSize)}
          >Update</Button
        >
      </div>
    </form>

    <div class="relative w-full flex-1">
      <div class="h-px border-b absolute -left-7 -right-7"></div>
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
