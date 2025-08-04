<script lang="ts">
  import Icon from '$lib/components/Icon.svelte';
  import { goto } from '$app/navigation';
  import { twMerge } from 'tailwind-merge';
  import { page } from '$app/state';
  import { openModal } from '$lib/components/Modals/AppModals.svelte';
  import Button from '$lib/components/Button.svelte';

  import { typedRoute } from '$lib/types/appRoutes';
  import { arrayMax } from '$lib/utils/arrayMax';
  import { onMount } from 'svelte';

  interface Props {
    data: any;
    children?: import('svelte').Snippet;
  }

  let { data, children }: Props = $props();

  let searchQuery = $state('');

  let filteredData = $derived(data.streams.filter((stream) => stream.name.includes(searchQuery)));

  onMount(() => {
    if (data.streams.length > 0 && page.url.pathname === typedRoute('/dashboard/streams')) {
      goto(typedRoute(`/dashboard/streams/${data.streams[0].id}`));
    }
  });

</script>

<div class="flex h-full flex-row">
  <div class="w-[290px] border-r h-full bg-shadeL200 dark:bg-shadeD900 flex flex-col">
    <div class="p-7 pl-9 py-5 border-b flex gap-2 relative">
      <Icon name="search" class="absolute text-gray-400 left-3 top-1/2 -translate-y-1/2 w-[20px]" />
      <input
        bind:value={searchQuery}
        placeholder="Search streams..."
        class="outline-none text-sm bg-transparent w-full ml-2 dark:text-white"
      />
    </div>

    {#if filteredData.length === 0}
      <div class="p-7 py-4 flex items-center justify-center text-sm text-gray-400">
        <em>No streams found.</em>
      </div>
    {/if}

    <ul class="flex-1 overflow-auto">
      {#each filteredData as { name, id, topicsCount, messagesCount, sizeBytes } (id)}
        {@const isActive = page.params.streamId === id.toString()}
        <li class="last:mb-6">
          <a
            href={typedRoute(`/dashboard/streams/${id}`)}
            class={twMerge(
              'flex w-full flex-col border-b gap-1 px-5 py-2 transition-colors  outline-none dark:text-white hoverable',
              isActive && 'bg-shadeL300 dark:bg-shadeD300'
            )}
          >
            <span
              class={twMerge(
                'font-semibold text-base truncate',
                isActive ? 'text-black dark:text-white' : 'text-shadeL1000  dark:text-shadeD100 '
              )}>{name}</span
            >
            <div class="grid grid-cols-2 w-full">
              <div class="flex flex-col items-start">
                <span class="text-xs">
                  Id: {id}
                </span>
                <span class="text-xs">Size: {sizeBytes}</span>
              </div>

              <div class="flex flex-col items-start">
                <span class="text-xs">Topics: {topicsCount}</span>
                <span class="text-xs">Messages: {messagesCount}</span>
              </div>
            </div>
          </a>
        </li>
      {/each}
    </ul>

    <div class="p-5 pb-7">
      <Button
        variant="outlined"
        class="w-full"
        onclick={() =>
          openModal('AddStreamModal', {
            nextStreamId: arrayMax(data.streams.map((s) => s.id)) + 1
          })}
      >
        <Icon name="plus" class="w-[16px] h-[16px]" strokeWidth={2} />
        New stream
      </Button>
    </div>
  </div>
  <div class="w-[calc(100%-290px)] h-full overflow-hidden flex flex-col">
    {@render children?.()}
  </div>
</div>
