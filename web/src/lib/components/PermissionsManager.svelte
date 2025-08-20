<script lang="ts" module>
  type GlobalPermissionsSnakeCase = keyof KeysToSnakeCase<GlobalPermissions>;
  type StreamPermissionsSnakeCase = Exclude<keyof KeysToSnakeCase<StreamPermissions>, 'topics'>;

  type TopicsPerms = Record<
    Topic['id'],
    Record<keyof TopicPermissions, { name: string; checked: false }>
  >;

  type StreamsPerms = Record<
    Stream['id'],
    Record<
      StreamPermissionsSnakeCase,
      {
        name: string;
        checked: boolean;
        disabled: boolean;
        globalPermsKey: GlobalPermissionsSnakeCase;
      }
    > & { topicPerms: TopicsPerms }
  >;

  type GlobalPerms = Record<
    GlobalPermissionsSnakeCase,
    { name: string; checked: boolean; relatesTo?: StreamPermissionsSnakeCase }
  >;
</script>

<script lang="ts">
  import { run } from 'svelte/legacy';

  import Icon from './Icon.svelte';
  import Combobox from './Combobox.svelte';
  import type { Stream } from '$lib/domain/Stream';
  import { topicMapper, type Topic } from '$lib/domain/Topic';
  import { fetchRouteApi } from '$lib/api/fetchRouteApi';
  import { showToast } from './AppToasts.svelte';
  import type { KeysToSnakeCase } from '$lib/utils/utilTypes';
  import type {
    GlobalPermissions,
    StreamPermissions,
    TopicPermissions
  } from '$lib/domain/Permissions';
  import Checkbox from './Checkbox.svelte';
  import { twMerge } from 'tailwind-merge';
  import { noTypeCheck } from '$lib/utils/noTypeCheck';
  import { fade } from 'svelte/transition';

  interface Props {
    streams: Stream[];
    value?: any;
  }

  let { streams, value = $bindable() }: Props = $props();

  let topics: Topic[] = $state([]);
  let fetchingTopics = $state(false);
  let selectedStream: { id: number; name: string } | undefined = $state(undefined);
  let selectedTopic: { id: number; name: string } | undefined = $state(undefined);

  if (streams.length > 0) {
    selectedStream = { name: streams[0].name, id: streams[0].id };
  }

  const fetchTopics = async (id: number) => {
    fetchingTopics = true;
    const { data, ok } = await fetchRouteApi({
      method: 'GET',
      path: `/streams/${id}/topics`
    });

    if (!ok) {
      showToast({ type: 'error', description: 'Something went wrong' });
      return;
    }
    fetchingTopics = false;
    const newTopics = data.map(topicMapper) as Topic[];

    if (newTopics.length > 0) {
      selectedTopic = { name: newTopics[0].name, id: newTopics[0].id };
    } else {
      selectedTopic = undefined;
    }

    topics = newTopics;
  };

  const buildTopicsPerms = (newTopics: Topic[]) => {
    const tempTopicPerms: TopicsPerms = {};

    if (!selectedStream || Object.keys(streamsPerms[selectedStream.id].topicPerms).length > 0) {
      return;
    }

    newTopics.forEach((t) => {
      tempTopicPerms[t.id] = {
        manageTopic: {
          checked: false,
          name: 'Manage topic'
        },
        pollMessages: {
          checked: false,
          name: 'Poll messages'
        },
        readTopic: {
          checked: false,
          name: 'Read topic'
        },
        sendMessages: {
          checked: false,
          name: 'Send messages'
        }
      };
    });

    streamsPerms[selectedStream.id].topicPerms = tempTopicPerms;
    streamsPerms = streamsPerms;
  };

  const onGlobalPermChanged = (key: GlobalPermissionsSnakeCase, checked: boolean) => {
    const relatesTo = globalPerms[key].relatesTo;

    if (relatesTo) {
      Object.keys(streamsPerms).forEach((k) => {
        streamsPerms[k][relatesTo] = { ...streamsPerms[k][relatesTo], checked, disabled: checked };
        streamsPerms = streamsPerms;
      });
    }
  };

  const globalPerms: GlobalPerms = $state({
    manage_servers: {
      name: 'Manage servers',
      checked: false
    },
    read_servers: {
      name: 'Read servers',
      checked: false
    },
    manage_users: {
      name: 'Manage users',
      checked: false
    },
    read_users: {
      name: 'Read users',
      checked: false
    },
    manage_streams: {
      name: 'Manage streams',
      relatesTo: 'manage_stream',
      checked: false
    },
    read_streams: {
      name: 'Read streams',
      relatesTo: 'read_stream',
      checked: false
    },
    manage_topics: {
      name: 'Manage topics',
      relatesTo: 'manage_topics',
      checked: false
    },
    read_topics: {
      name: 'Read topics',
      relatesTo: 'read_topics',
      checked: false
    },
    poll_messages: {
      name: 'Pool messages',
      relatesTo: 'poll_messages',
      checked: false
    },
    send_messages: {
      name: 'Send messages',
      relatesTo: 'send_messages',
      checked: false
    }
  });

  let streamsPerms = $state((() => {
    const tempPerms: StreamsPerms = {};

    streams.forEach((s) => {
      tempPerms[s.id] = {
        manage_stream: {
          name: 'Manage stream',
          globalPermsKey: 'manage_streams',
          checked: false,
          disabled: false
        },
        read_stream: {
          name: 'Read stream',
          globalPermsKey: 'read_streams',
          checked: false,
          disabled: false
        },
        read_topics: {
          name: 'Read topics',
          globalPermsKey: 'read_topics',
          checked: false,
          disabled: false
        },
        poll_messages: {
          name: 'Poll messages',
          globalPermsKey: 'poll_messages',
          checked: false,
          disabled: false
        },
        send_messages: {
          name: 'Send messages',
          globalPermsKey: 'send_messages',
          checked: false,
          disabled: false
        },
        manage_topics: {
          name: 'Manage topics',
          globalPermsKey: 'manage_topics',
          checked: false,
          disabled: false
        },
        topicPerms: {}
      };
    });

    return tempPerms;
  })() satisfies StreamsPerms);

  run(() => {
    if (selectedStream) fetchTopics(selectedStream.id);
  });
  run(() => {
    buildTopicsPerms(topics);
  });

  let taintedStreams = $derived((() => {
    const tainted: Set<number> = new Set([]);

    Object.keys(streamsPerms).forEach((streamId) => {
      Object.keys(streamsPerms[streamId]).forEach((permissionKey) => {
        if (permissionKey === 'topicPerms') {
          const perm = streamsPerms[streamId][permissionKey];

          Object.keys(perm).forEach((topicId) => {
            const topicPerm = perm[topicId];
            const isTopicTained = Object.keys(topicPerm)
              .map((k) => topicPerm[k])
              .some((p) => p.checked);

            if (isTopicTained) tainted.add(streamId);
          });
        } else {
          const perm = streamsPerms[streamId][permissionKey];
          if (perm.checked && !perm.disabled) tainted.add(streamId);
        }
      });
    });

    return Array.from(tainted);
  })().map((taintedStreamId) => {
    const name = streams.find((stream) => stream.id === +taintedStreamId)!.name;
    return { name, id: +taintedStreamId };
  }));

  $effect(() => {
    function formatGlobalPermissions() {
      return Object.keys(globalPerms).reduce((result, key) => {
        result[key] = globalPerms[key].checked;
        return result;
      }, {});
    }

    function hasAnyPermissionChecked(permissionsObj: Record<string, any>, excludeKeys: string[] = []) {
      return Object.keys(permissionsObj)
        .filter(key => !excludeKeys.includes(key))
        .some(key => permissionsObj[key].checked);
    }

    function formatTopicPermissions(topicPerms: Record<string, any>) {
      const result = {};

      Object.keys(topicPerms).forEach(topicId => {
        const topicPerm = topicPerms[topicId];

        if (!hasAnyPermissionChecked(topicPerm)) {
          return;
        }

        result[topicId] = {
          manage_topic: topicPerm.manageTopic.checked,
          read_topic: topicPerm.readTopic.checked,
          poll_messages: topicPerm.pollMessages.checked,
          send_messages: topicPerm.sendMessages.checked
        };
      });

      return result;
    }

    function formatStreamPermissions() {
      const result = {};

      Object.keys(streamsPerms).forEach(streamId => {
        const streamPerm = streamsPerms[streamId];
        const topicPerms = streamPerm.topicPerms;

        const hasStreamPermissions = hasAnyPermissionChecked(streamPerm, ["topicPerms"]);

        const formattedTopics = formatTopicPermissions(topicPerms);
        const hasTopicPermissions = Object.keys(formattedTopics).length > 0;

        if (!hasStreamPermissions && !hasTopicPermissions) {
          return;
        }

        result[streamId] = {
          manage_stream: streamPerm.manage_stream.checked,
          read_stream: streamPerm.read_stream.checked,
          manage_topics: streamPerm.manage_topics.checked,
          read_topics: streamPerm.read_topics.checked,
          poll_messages: streamPerm.poll_messages.checked,
          send_messages: streamPerm.send_messages.checked
        };

        if (hasTopicPermissions) {
          result[streamId].topics = formattedTopics;
        }
      });

      return result;
    }

    value = {
      global: formatGlobalPermissions(),
      streams: formatStreamPermissions()
    };
  });
</script>

<h4 class="ml-1 text-lg text-color mt-7">Global permissions</h4>

<div class="grid grid-cols-4 mt-4">
  {#each Object.keys(globalPerms) as key (key)}
    <label
      class="flex gap-2 items-center text-color cursor-pointer"
      for={`global-permissions-${key}`}
    >
      <Checkbox
        bind:checked={globalPerms[key].checked}
        value={globalPerms[key].name}
        id={`global-permissions-${key}`}
        name={globalPerms[key].name}
        onclick={(e) => onGlobalPermChanged(key, noTypeCheck(e).target.checked)}
      />
      <span class="text-sm">{globalPerms[key].name}</span>
    </label>
  {/each}
</div>

{#if selectedStream}
  <div class="flex flex-wrap gap-3 mt-6 items-center">
    <h4 class="text-lg text-color mr-2">Granular permissions</h4>

    {#each taintedStreams as { id, name } (id)}
      <button
        type="button"
        onclick={() => (selectedStream = { name, id })}
        transition:fade={{ duration: 80 }}
        class={twMerge(
          'rounded-3xl px-3 py-1 whitespace-nowrap text-xs hover:shadow-lg  hover:ring-2 transition-all dark:text-white ring-1 ring-green-500 shadow-md hover:cursor-pointer',
          selectedStream?.id === id && 'bg-green-500 text-white'
        )}
        >id: {id}, {name}
      </button>
    {/each}
  </div>

  <div class="grid grid-cols-[1fr_auto_1fr] gap-5 mt-4">
    <div class="w-full flex flex-col">
      <Combobox
        items={streams}
        formatter={(item) => `id: ${item.id}, ${item.name}`}
        label={`Stream`}
        bind:selectedValue={selectedStream}
      />

      <div class="grid grid-cols-2 mt-4">
        {#each Object.keys(streamsPerms[selectedStream.id]) as key (key)}
          {#if key !== 'topicPerms'}
            <label
              class={twMerge(
                'flex gap-2 items-center text-color cursor-pointer',
                streamsPerms[selectedStream.id][key].disabled && 'cursor-not-allowed text-shadeL800'
              )}
              for={`stream-${key}-permission`}
            >
              <Checkbox
                bind:checked={streamsPerms[selectedStream.id][key].checked}
                value={streamsPerms[selectedStream.id][key].name}
                disabled={streamsPerms[selectedStream.id][key].disabled}
                id={`stream-${key}-permission`}
              />
              <span class={twMerge('text-sm')}>{streamsPerms[selectedStream.id][key].name}</span>
            </label>
          {/if}
        {/each}
      </div>
    </div>

    <div class="h-[68px] w-[40px] flex flex-col justify-end">
      <div class="w-fit h-fit">
        <Icon name="chevronRight" class="h-[40px] dark:stroke-white mt-auto  w-auto" />
      </div>
    </div>

    <div class="w-full flex flex-col">
      {#if topics.length === 0 && !streamsPerms[selectedStream.id].manage_topics.checked}
        <em class="italic dark:text-white text-center block mt-[34px]">
          This stream has no topics.
        </em>
      {/if}
      {#if streamsPerms[selectedStream.id].manage_topics.checked}
        <div class="dark:text-white mt-9 text-center">
          <span> Every topic in stream </span>
          <em class="text-green-500">{selectedStream.name}</em>
          <span> {topics.length === 0 ? 'will have' : 'has'} full permissions </span>
        </div>
      {/if}

      {#if selectedTopic && Object.keys(streamsPerms[selectedStream.id].topicPerms).length > 0 && !streamsPerms[selectedStream.id].manage_topics.checked}
        <div>
          <Combobox
            isLoading={fetchingTopics}
            items={topics}
            formatter={(item) => `id: ${item.id}, ${item.name}`}
            label="Topic"
            bind:selectedValue={selectedTopic}
          />

          <div class="grid grid-cols-2 mt-4">
            {#each Object.keys(streamsPerms[selectedStream.id].topicPerms[selectedTopic.id]) as key (key)}
              <label class="flex gap-2 items-center text-color cursor-pointer">
                <Checkbox
                  bind:checked={streamsPerms[selectedStream.id].topicPerms[selectedTopic.id][key]
                    .checked}
                  value={''}
                />
                <span class="text-sm"
                  >{streamsPerms[selectedStream.id].topicPerms[selectedTopic.id][key].name}</span
                >
              </label>
            {/each}
          </div>
        </div>
      {/if}
    </div>
  </div>
{/if}
