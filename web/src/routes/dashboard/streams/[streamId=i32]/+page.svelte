<script lang="ts">
  import { page } from '$app/state';
  import Button from '$lib/components/Button.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import { openModal } from '$lib/components/Modals/AppModals.svelte';
  import SortableList from '$lib/components/SortableList.svelte';
  import { typedRoute } from '$lib/types/appRoutes';
  import type { StreamDetails } from '$lib/domain/StreamDetails';

  interface Props {
    data: {
      streamDetails: StreamDetails;
    };
  }

  let { data }: Props = $props();
  let stream = $derived(data.streamDetails);
</script>

<div class="h-[80px] flex flex-row text-xs items-center px-7">
  <h1 class="font-semibold text-xl dark:text-white max-w-[380px] break-words line-clamp-3">
    Stream {stream.name}
  </h1>

  <Button
    variant="rounded"
    class="ml-3"
    onclick={() => openModal('StreamSettingsModal', { stream })}
  >
    <Icon name="settings" class="dark:text-white" />
    {#snippet tooltip()}
      <div>Settings</div>
    {/snippet}
  </Button>

  <div class="flex gap-3 ml-7">
    <div class="chip">
      <span>Id: {stream.id}</span>
    </div>
    <div class="chip">
      <span>Size: {stream.sizeFormatted}</span>
    </div>
    <div class="chip">
      <span>Messages: {stream.messagesCount}</span>
    </div>
    <div class="chip">
      <span>Topics: {stream.topicsCount}</span>
    </div>
  </div>

  <Button
    variant="contained"
    class="ml-auto"
    onclick={() =>
      openModal('AddTopicModal', {
        streamDetails: stream
      })}
  >
    <Icon name="plus" class="w-[16px] h-[16px]" strokeWidth={2} />
    Add Topic
  </Button>
</div>

<SortableList
  emptyDataMessage="This stream has no topics."
  rowClass="grid grid-cols-[150px_3fr_2fr_2fr_2fr_2fr_3fr]"
  data={stream.topics}
  hrefBuilder={(topic) =>
    typedRoute(`/dashboard/streams/${+(page.params.streamId || '')}/topics/${topic.id}`)}
  colNames={{
    id: 'ID',
    name: 'Name',
    messagesCount: 'Messages',
    partitionsCount: 'Partitions',
    messageExpiryFormatted: 'Message expiry',
    sizeFormatted: 'Size',
    createdAt: 'Created',
    sizeBytes: undefined
  }}
/>
