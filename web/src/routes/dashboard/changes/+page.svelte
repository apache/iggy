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
  import { page } from '$app/state';
  import Button from '#lib/components/Button.svelte';
  import Checkbox from '#lib/components/Checkbox.svelte';
  import { authStore } from '#lib/auth/authStore.svelte.js';
  import { fetchMetadataChanges } from '#lib/api/fetchMetadataChanges.js';
  import {
    DEFAULT_METADATA_CHANGES_LIMIT,
    METADATA_CHANGE_GROUPS,
    MAX_SHOWN_METADATA_CHANGES,
    scanRecentMetadataChanges,
    canReadMetadataChanges,
    describeChange,
    filterProblem,
    metadataChangesQuery,
    microsFromDateTimeLocal,
    statusMessage,
    type MetadataChangeGroup,
    type MetadataChangeRecord,
    type MetadataChangesFilter,
    type MetadataChangesPage
  } from '#lib/api/metadataChanges.js';
  import { onDestroy, onMount } from 'svelte';
  import { fly } from 'svelte/transition';

  const FOLLOW_INTERVAL_MILLIS = 2000;
  const inputClass =
    'rounded-md dark:bg-shade-d400 ring-1 ring-gray-300 dark:ring-gray-500 h-[40px] px-3 text-color bg-transparent';

  let selectedRecord = $state<MetadataChangeRecord | null>(null);
  let copyFeedback = $state('');
  let copyFeedbackTimer: ReturnType<typeof setTimeout> | undefined;
  let pageContainer: HTMLDivElement;
  const selectedOptions = $derived(
    selectedRecord?.options && typeof selectedRecord.options === 'object'
      ? Object.entries(selectedRecord.options as Record<string, unknown>).map(([key, option]) => {
          const field =
            option && typeof option === 'object' ? (option as Record<string, unknown>) : {};
          return {
            key,
            value: 'value' in field ? field.value : option,
            kind: field.kind,
            explicit: field.explicit
          };
        })
      : []
  );

  function inspect(record: MetadataChangeRecord) {
    selectedRecord = record;
    copyFeedback = '';
    clearTimeout(copyFeedbackTimer);
  }

  function openDetails(dialog: HTMLDialogElement) {
    const trigger = document.activeElement;
    const overflow = pageContainer.style.overflow;
    pageContainer.style.overflow = 'hidden';
    dialog.showModal();
    return {
      destroy() {
        dialog.close();
        pageContainer.style.overflow = overflow;
        if (trigger instanceof HTMLElement) trigger.focus({ preventScroll: true });
      }
    };
  }

  function closeOnBackdrop(event: MouseEvent) {
    if (event.target !== event.currentTarget) return;
    const bounds = (event.currentTarget as HTMLDialogElement).getBoundingClientRect();
    if (
      event.clientX < bounds.left ||
      event.clientX > bounds.right ||
      event.clientY < bounds.top ||
      event.clientY > bounds.bottom
    ) {
      selectedRecord = null;
    }
  }

  function displayValue(value: unknown): string {
    return typeof value === 'string' ? value : (JSON.stringify(value, null, 2) ?? '');
  }

  async function copy(value: unknown, label: string) {
    clearTimeout(copyFeedbackTimer);
    try {
      await navigator.clipboard.writeText(displayValue(value));
      copyFeedback = `${label} copied`;
    } catch {
      copyFeedback = 'Copy is unavailable. You can select and copy the text below.';
    }
    copyFeedbackTimer = setTimeout(() => {
      copyFeedback = '';
    }, 3000);
  }

  let start = $state<'earliest' | 'recent'>('earliest');
  const order = $derived(start === 'recent' ? 'newest' : 'oldest');
  let recentLoaded = $state(false);
  let visibleCount = $state(DEFAULT_METADATA_CHANGES_LIMIT);
  let scannedPages = $state(0);
  let recentController = $state<AbortController | undefined>();
  let groups = $state<string[]>([]);
  let since = $state('');
  let until = $state('');
  let limit = $state(DEFAULT_METADATA_CHANGES_LIMIT);
  let records = $state<MetadataChangeRecord[]>([]);
  const shownRecords = $derived(
    (order === 'newest' ? records.slice().reverse() : records).slice(
      0,
      recentLoaded ? visibleCount : records.length
    )
  );
  let cursor = $state<string | null>(null);
  let hasMore = $state(false);
  let lastPage = $state<MetadataChangesPage | null>(null);
  let notice = $state<string | undefined>(undefined);
  let problem = $state<string | undefined>(undefined);
  let forbidden = $state(false);
  let loading = $state(false);
  let following = $state(false);
  let activeFilter: MetadataChangesFilter | null = null;
  let followTimer: ReturnType<typeof setInterval> | undefined;

  let allowed = $derived(
    canReadMetadataChanges(authStore.userId, page.data.user?.permissions?.global?.manageServers)
  );

  function currentFilter(): MetadataChangesFilter {
    return {
      start: start === 'recent' ? 'earliest' : start,
      groups: groups as MetadataChangeGroup[],
      since: microsFromDateTimeLocal(since),
      until: microsFromDateTimeLocal(until),
      limit: Number(limit)
    };
  }

  async function read(filter: MetadataChangesFilter, after: string | null) {
    loading = true;
    const result = await fetchMetadataChanges(metadataChangesQuery(filter, after ?? undefined));
    loading = false;
    switch (result.kind) {
      case 'forbidden':
        forbidden = true;
        stopFollowing();
        return;
      case 'refused':
      case 'failed':
        problem = result.reason;
        stopFollowing();
        return;
      case 'page':
        break;
    }
    const answer = result.page;
    lastPage = answer;
    notice = statusMessage(answer.status, following);
    if (answer.status === 'ok') {
      records = [...records, ...answer.records].slice(-MAX_SHOWN_METADATA_CHANGES);
      cursor = answer.next;
      hasMore = answer.has_more;
    } else if (answer.status !== 'behind') {
      cursor = null;
      hasMore = false;
      stopFollowing();
    }
  }

  async function load() {
    const filter = currentFilter();
    problem = filterProblem(filter);
    if (problem) {
      return;
    }
    stopFollowing();
    activeFilter = filter;
    records = [];
    cursor = null;
    hasMore = false;
    lastPage = null;
    notice = undefined;
    recentLoaded = start === 'recent';
    visibleCount = filter.limit;
    if (!recentLoaded) {
      await read(filter, null);
      return;
    }
    loading = true;
    scannedPages = 0;
    const controller = new AbortController();
    recentController = controller;
    try {
      const result = await scanRecentMetadataChanges(
        filter,
        async (query) => {
          const response = await fetchMetadataChanges(query, controller.signal);
          if (response.kind === 'page') return response.page;
          if (response.kind === 'forbidden') {
            forbidden = true;
            throw new Error('Reading metadata changes is not allowed.');
          }
          throw new Error(response.reason);
        },
        {
          signal: controller.signal,
          onPage: (count) => {
            scannedPages = count;
          }
        }
      );
      lastPage = result.page;
      if (result.page.status === 'ok') {
        records = result.page.records;
        cursor = result.page.next;
        if (result.trimmed)
          notice = `Showing the newest ${MAX_SHOWN_METADATA_CHANGES} matching records. Narrow the time range or load from the earliest retained change to browse further back.`;
      } else {
        notice =
          result.page.status === 'behind'
            ? 'This node is behind the captured position. Load again shortly.'
            : statusMessage(result.page.status, false);
      }
    } catch (error) {
      if (controller.signal.aborted) notice = 'Loading canceled.';
      else problem = error instanceof Error ? error.message : 'Could not load recent changes.';
    } finally {
      recentController = undefined;
      loading = false;
    }
  }

  async function loadMore() {
    if (recentLoaded) {
      visibleCount = Math.min(records.length, visibleCount + (activeFilter?.limit ?? limit));
    } else {
      await pollChanges();
    }
  }

  async function pollChanges() {
    if (activeFilter && cursor && !loading) {
      await read(activeFilter, cursor);
    }
  }

  function startFollowing() {
    following = true;
    followTimer = setInterval(pollChanges, FOLLOW_INTERVAL_MILLIS);
  }

  function stopFollowing() {
    following = false;
    clearInterval(followTimer);
    followTimer = undefined;
  }

  onMount(() => {
    if (allowed) void load();
  });

  onDestroy(() => {
    stopFollowing();
    recentController?.abort();
    clearTimeout(copyFeedbackTimer);
  });
</script>

<div bind:this={pageContainer} class="p-5 flex flex-col gap-5 overflow-auto h-full text-color">
  <h1 class="text-2xl font-semibold">Metadata changes</h1>

  {#if !allowed || forbidden}
    <p class="text-shade-d200 dark:text-shade-l700">
      Reading metadata changes needs the root user or the global manage servers permission.
    </p>
  {:else}
    <section class="border rounded-md p-5 flex flex-col gap-4">
      <div class="flex flex-wrap gap-5 items-end">
        <label class="flex flex-col gap-2 text-base font-medium">
          Order
          <select
            value={start}
            aria-label="Order"
            class="{inputClass} cursor-pointer"
            disabled={loading}
            onchange={(event) => {
              start = event.currentTarget.value as 'earliest' | 'recent';
              void load();
            }}
          >
            <option value="recent">Newest first</option>
            <option value="earliest">Oldest first</option>
          </select>
        </label>
        <label class="flex flex-col gap-2 text-base font-medium">
          Since
          <input type="datetime-local" bind:value={since} class={inputClass} />
        </label>
        <label class="flex flex-col gap-2 text-base font-medium">
          Until
          <input type="datetime-local" bind:value={until} class={inputClass} />
        </label>
        <label class="flex flex-col gap-2 text-base font-medium">
          Page size
          <input
            type="number"
            min="1"
            max="1000"
            bind:value={limit}
            class="{inputClass} w-[110px]"
          />
        </label>
      </div>
      <fieldset class="flex flex-wrap gap-4 text-base">
        <legend class="mb-2">Groups (none selected means all)</legend>
        {#each METADATA_CHANGE_GROUPS as group (group)}
          <label class="flex items-center gap-2 cursor-pointer">
            <Checkbox value={group} bind:bindGroup={groups} />
            {group.replaceAll('_', ' ')}
          </label>
        {/each}
      </fieldset>
      <div class="flex gap-3 items-center">
        <Button variant="contained" onclick={load} disabled={loading}>Refresh</Button>
        <Button
          variant="outlined"
          onclick={loadMore}
          disabled={loading ||
            (recentLoaded ? shownRecords.length >= records.length : !cursor || !hasMore)}
        >
          Show more
        </Button>
        <Button
          variant="outlined"
          onclick={() => (following ? stopFollowing() : startFollowing())}
          disabled={!cursor && !following}
        >
          {following ? 'Stop following' : 'Follow'}
        </Button>
        {#if recentController}
          <Button variant="outlined" onclick={() => recentController?.abort()}>Cancel</Button>
          <span role="status" class="text-sm"
            >Loading recent changes, {scannedPages} pages read</span
          >
        {/if}
        {#if lastPage}
          <span class="text-sm">
            Showing {shownRecords.length} of {records.length} loaded changes
          </span>
        {/if}
      </div>
      {#if start === 'recent'}
        <p class="text-base leading-6 text-gray-700 dark:text-gray-200">
          Showing up to {MAX_SHOWN_METADATA_CHANGES.toLocaleString()} recent changes. Choose Oldest first
          to browse from the beginning.
        </p>
      {/if}
      {#if problem}
        <p class="text-red-600 text-sm">{problem}</p>
      {/if}
      {#if notice}
        <p class="text-sm">{notice}</p>
      {/if}
    </section>

    <table class="w-full text-base border-collapse">
      <thead>
        <tr class="text-left border-b">
          <th class="py-2 pr-4">Operation</th>
          <th class="py-2 pr-4">Time</th>
          <th class="py-2 pr-4">Kind</th>
          <th class="py-2 pr-4">Actor</th>
          <th class="py-2">Change</th>
        </tr>
      </thead>
      <tbody>
        {#each shownRecords as record (`${record.op}:${record.ordinal}`)}
          <tr
            class="border-b align-top transition-colors hover:bg-shade-l300 dark:hover:bg-shade-d600"
          >
            <td class="py-2 pr-4">
              <span class="block font-mono">{record.op}</span>
              <span class="text-sm text-gray-700 dark:text-gray-200">Ordinal {record.ordinal}</span>
            </td>
            <td class="py-2 pr-4 whitespace-nowrap">
              {new Date(Number(record.timestamp) / 1000).toLocaleString()}
            </td>
            <td class="py-3 pr-4"
              ><span
                class="inline-block rounded-full bg-shade-l300 dark:bg-shade-d500 px-2.5 py-1 text-sm whitespace-nowrap"
                >{record.kind.replaceAll('_', ' ')}</span
              ></td
            >
            <td class="py-2 pr-4">
              {record.actor_user_id === undefined ? 'server' : `user ${record.actor_user_id}`}
            </td>
            <td class="py-2">
              <button
                class="group cursor-pointer flex w-full items-center justify-between gap-4 rounded-md px-2 py-1 text-left hover:bg-shade-l300 dark:hover:bg-shade-d500 focus-visible:outline-2 focus-visible:outline-offset-2"
                onclick={() => inspect(record)}
                aria-label={`Inspect ${record.kind.replaceAll('_', ' ')} at operation ${record.op}, ordinal ${record.ordinal}`}
                aria-haspopup="dialog"
              >
                <span class="break-words">{describeChange(record)}</span>
                <span
                  class="shrink-0 text-sm text-gray-700 dark:text-gray-200 group-hover:text-current"
                  >Details</span
                >
              </button>
            </td>
          </tr>
        {:else}
          <tr>
            <td colspan="5" class="py-5 text-center text-gray-700 dark:text-gray-200">
              {loading ? 'Loading changes...' : 'No changes match these filters.'}
            </td>
          </tr>
        {/each}
      </tbody>
    </table>
  {/if}
</div>

{#if selectedRecord}
  <dialog
    use:openDetails
    transition:fly={{ x: 16, duration: 120 }}
    class="details-panel fixed inset-y-0 left-auto right-0 m-0 h-dvh max-h-none w-full max-w-2xl border-0 bg-shade-l100 dark:bg-shade-d700 p-0 text-color shadow-2xl"
    aria-labelledby="change-details-title"
    oncancel={(event) => {
      event.preventDefault();
      selectedRecord = null;
    }}
    onclick={closeOnBackdrop}
  >
    <div class="flex h-full flex-col">
      <header class="shrink-0 border-b p-6">
        <div class="mb-4 flex items-center justify-between gap-4">
          <span
            class="text-sm font-medium uppercase tracking-wider text-gray-700 dark:text-gray-200"
            >Record details</span
          >
          <button
            class="cursor-pointer rounded-lg px-3 py-2 text-sm hover:bg-shade-l300 dark:hover:bg-shade-d500"
            onclick={() => {
              selectedRecord = null;
            }}
            aria-label="Close record details">Close ×</button
          >
        </div>
        <h2 id="change-details-title" class="text-xl font-semibold">
          {selectedRecord.kind.replaceAll('_', ' ')}
        </h2>
        <p class="mt-2 break-words text-sm text-gray-700 dark:text-gray-200">
          {describeChange(selectedRecord)}
        </p>
      </header>
      <div class="min-h-0 flex-1 overflow-auto overscroll-contain p-6">
        <dl
          class="mb-6 grid grid-cols-2 gap-4 rounded-xl border bg-white dark:bg-shade-d600 p-4 text-sm"
        >
          <div>
            <dt class="mb-1 text-sm text-gray-700 dark:text-gray-200">Operation</dt>
            <dd class="font-mono">{selectedRecord.op}</dd>
          </div>
          <div>
            <dt class="mb-1 text-sm text-gray-700 dark:text-gray-200">Record ordinal</dt>
            <dd class="font-mono">{selectedRecord.ordinal}</dd>
          </div>
          <div>
            <dt class="mb-1 text-sm text-gray-700 dark:text-gray-200">Actor</dt>
            <dd>
              {selectedRecord.actor_user_id === undefined
                ? 'Server'
                : `User ${selectedRecord.actor_user_id}`}
            </dd>
          </div>
          <div class="col-span-2">
            <dt class="mb-1 text-sm text-gray-700 dark:text-gray-200">Time</dt>
            <dd>{new Date(Number(selectedRecord.timestamp) / 1000).toLocaleString()}</dd>
          </div>
        </dl>
        {#if selectedOptions.length}
          <section class="mb-6">
            <h3 class="mb-3 text-sm font-semibold">Options</h3>
            <div class="overflow-hidden rounded-xl border bg-white dark:bg-shade-d600">
              {#each selectedOptions as option (option.key)}
                <div class="border-b p-4 last:border-b-0">
                  <div class="mb-2 flex flex-wrap items-center gap-2">
                    <span class="font-mono text-sm font-medium">{option.key}</span>
                    {#if option.kind}<span
                        class="rounded bg-shade-l300 dark:bg-shade-d500 px-1.5 py-0.5 text-sm text-gray-700 dark:text-gray-200"
                        >{String(option.kind)}</span
                      >{/if}
                    {#if typeof option.explicit === 'boolean'}<span
                        class="text-sm text-gray-700 dark:text-gray-200"
                        >{option.explicit ? 'Explicit' : 'Default / derived'}</span
                      >{/if}
                    <button
                      class="cursor-pointer ml-auto rounded px-2 py-1 text-sm hover:bg-shade-l300 dark:hover:bg-shade-d500"
                      onclick={() => copy(option.value, option.key)}
                      aria-label={`Copy ${option.key}`}>Copy value</button
                    >
                  </div>
                  <pre
                    class="whitespace-pre-wrap break-all font-mono text-sm leading-relaxed">{displayValue(
                      option.value
                    )}</pre>
                </div>
              {/each}
            </div>
          </section>
        {/if}
        <section>
          <h3 class="mb-3 text-sm font-semibold">Full payload</h3>
          <pre
            class="overflow-x-auto rounded-xl border bg-white dark:bg-shade-d800 p-4 font-mono text-sm leading-6"
            aria-label="Full JSON payload">{JSON.stringify(selectedRecord, null, 2)}</pre>
          <div class="mt-4 flex items-center gap-3">
            <Button variant="contained" onclick={() => copy(selectedRecord, 'Payload')}
              >Copy payload</Button
            >
            <span role="status" class="text-sm text-gray-700 dark:text-gray-200"
              >{copyFeedback}</span
            >
          </div>
        </section>
      </div>
    </div>
  </dialog>
{/if}

<style>
  .details-panel::backdrop {
    background: rgb(0 0 0 / 25%);
  }

  @media (prefers-reduced-motion: reduce) {
    .details-panel {
      animation-duration: 0.01ms !important;
    }
  }
</style>
