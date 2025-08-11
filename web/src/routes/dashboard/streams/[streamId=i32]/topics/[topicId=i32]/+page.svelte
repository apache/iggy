<script lang="ts">
  import { page } from '$app/state';
  import Button from '$lib/components/Button.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import { goto } from '$app/navigation';
  import { typedRoute } from '$lib/types/appRoutes';
  import { openModal } from '$lib/components/Modals/AppModals.svelte';
  import SortableList from '$lib/components/SortableList.svelte';
  import type { Topic } from '$lib/domain/Topic';
  import type { Partition } from '$lib/domain/Partition';

  interface Props {
    data: {
      topic: Topic & {
        partitions: Partition[];
      };
    }
  }

  let { data }: Props = $props();
  let topic = $derived(data.topic);
  let prevPage = $derived(page.url.pathname.split('/').slice(0, 4).join('/') + '/');
</script>

<div class="h-[80px] flex text-xs items-center pl-2 pr-5">
  <Button variant="rounded" class="mr-5" onclick={() => goto(prevPage)}>
    <Icon name="arrowLeft" class="h-[40px] w-[30px]" />
  </Button>

  <h1 class="text-xl font-semibold text-color">Topic {topic.name}</h1>

  <Button
    variant="rounded"
    class="ml-3"
    onclick={() => openModal('TopicSettingsModal', { topic, onDeleteRedirectPath: prevPage })}
  >
    {#snippet children()}
      <Icon name="settings" class="dark:text-white" />
    {/snippet}
    {#snippet tooltip()}
      <div>Settings</div>
    {/snippet}
  </Button>

  <div class="flex gap-3 ml-7">
    <div class="chip">
      <span>Id: {topic.id}</span>
    </div>
    <div class="chip">
      <span>Size: {topic.sizeFormatted}</span>
    </div>
    <div class="chip">
      <span>Messages: {topic.messagesCount}</span>
    </div>
    <div class="chip">
      <span>Partitions: {topic.partitionsCount}</span>
    </div>
  </div>

  <div class="flex gap-2 ml-auto">
    {#if topic.partitions.length > 0}
      <Button variant="outlinedRed" onclick={() => openModal('DeletePartitionsModal', { topic })}
        >Delete partitions</Button
      >
    {/if}
    <Button variant="contained" onclick={() => openModal('AddPartitionsModal')}
      >Add partitions</Button
    >
  </div>
</div>

<SortableList
  emptyDataMessage="No partitions found."
  rowClass="grid grid-cols-[150px_1fr_1fr_1fr_1fr_1fr]"
  data={topic.partitions}
  hrefBuilder={(partition) =>
    typedRoute(
      `/dashboard/streams/${+(page.params.streamId || '')}/topics/${topic.id}/partitions/${partition.id}/messages`
    )}
  colNames={{
    id: 'ID',
    currentOffset: 'Offset',
    segmentsCount: 'Segments',
    messagesCount: 'Messages',
    sizeFormatted: 'Size',
    createdAt: 'Created',
    sizeBytes: undefined
  }}
/>
