<script lang="ts">
  import { page } from '$app/state';
  import Button from '$lib/components/Button.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import { goto } from '$app/navigation';
  import { openModal } from '$lib/components/Modals/AppModals.svelte';
  import SortableList from '$lib/components/SortableList.svelte';
  import Paginator from '$lib/components/Paginator.svelte';

  interface Props {
    data: any;
  }

  let { data }: Props = $props();
  let topic = $derived(data.topic);
  let partitionMessages = $derived(data.partitionMessages);
  let prevPage = $derived(page.url.pathname.split('/').slice(0, 6).join('/') + '/');

  let direction = $state(page.url.searchParams.get('direction') || 'desc');
  let currentPage = $state(1);
  let totalPages = $derived(
    Math.ceil((partitionMessages.currentOffset + 1) / data.pagination.count)
  );

  async function loadPage(page: number) {
    const messagesPerPage = data.pagination.count;
    const totalMessages = partitionMessages.currentOffset + 1;

    let offset: number;
    if (direction === 'desc') {
      offset = Math.max(0, totalMessages - page * messagesPerPage);
    } else {
      offset = (page - 1) * messagesPerPage;
    }

    const url = new URL(window.location.href);
    url.searchParams.set('offset', offset.toString());
    url.searchParams.set('direction', direction);
    await goto(url, { keepFocus: true, noScroll: true });
    currentPage = page;
  }

  function onPageChange(event: CustomEvent<number>) {
    const page = event.detail;
    loadPage(page);
  }

  function toggleDirection() {
    direction = direction === 'desc' ? 'asc' : 'desc';
    loadPage(1);
  }
</script>

<div class="h-[80px] flex text-xs items-center pl-2 pr-5">
  <Button variant="rounded" class="mr-5" on:click={() => goto(prevPage)}>
    <Icon name="arrowLeft" class="h-[40px] w-[30px]" />
  </Button>

  <h1 class="text-xl font-semibold text-color">
    Messages for {topic.name}, partition {partitionMessages.partitionId}
  </h1>

  <div class="flex gap-3 ml-7">
    <div class="chip">
      <span>Messages: {partitionMessages.currentOffset}</span>
    </div>
  </div>

  <div class="flex gap-2 ml-auto">
    <Button variant="contained" on:click={toggleDirection}>
      <Icon name={direction === 'desc' ? 'arrowDown' : 'arrowUp'} class="h-5 w-5 mr-2" />
      {direction === 'desc' ? 'Newest first' : 'Oldest first'}
    </Button>
  </div>
</div>

<div class="flex-1 overflow-auto">
  {#if partitionMessages.messages.length === 0}
    <div class="flex items-center justify-center h-full text-gray-400 text-xl">
      <em>No messages found.</em>
    </div>
  {:else}
    <SortableList
      emptyDataMessage="No messages found."
      rowClass="grid grid-cols-[150px_2fr_1fr_1fr_1fr]"
      data={direction === 'desc'
        ? [...partitionMessages.messages].reverse()
        : partitionMessages.messages}
      onclickAction={(index) =>
        openModal('InspectMessage', {
          message:
            partitionMessages.messages[
              direction === 'desc' ? partitionMessages.messages.length - 1 - index : index
            ]
        })}
      ariaRoleDescription="Display message details"
      colNames={{
        offset: 'Offset',
        truncatedPayload: 'Payload',
        formattedTimestamp: 'Timestamp',
        checksum: 'Checksum'
      }}
    />
  {/if}
</div>

<div class="mt-2 mb-2">
  <Paginator {currentPage} {totalPages} maxVisiblePages={5} on:pageChange={onPageChange} />
</div>
