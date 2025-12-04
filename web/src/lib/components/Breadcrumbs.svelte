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
  import { typedRoute } from '$lib/types/appRoutes';
  import Icon from './Icon.svelte';
  import { isNumber } from '$lib/utils/parsers';
  import { twMerge } from 'tailwind-merge';
  import { resolve } from '$app/paths';

  type Crumb = {
    path: string;
    label: string;
  };

  const MAX_NAME_LENGTH = 20;

  let streams = $derived(page.data.streams ?? []);
  let currentStream = $derived(page.data.streamDetails);
  let currentTopic = $derived(page.data.topic);

  function truncateName(name: string): string {
    return name.length > MAX_NAME_LENGTH ? `${name.slice(0, MAX_NAME_LENGTH)}...` : name;
  }

  function formatStreamName(streamId: string): string {
    const stream = currentStream || streams?.find((str: any) => str.id.toString() === streamId);
    return stream ? truncateName(stream.name) : streamId;
  }

  function formatTopicName(topicId: string): string {
    return currentTopic ? truncateName(currentTopic.name) : topicId;
  }

  function formatPathSegment(segment: string, index: number, parts: string[]): Crumb {
    const path = `/dashboard/${parts.slice(0, index + 1).join('/')}`;

    if (isNumber(segment)) {
      const prevSegment = parts[index - 1];
      switch (prevSegment) {
        case 'streams':
          return { path, label: formatStreamName(segment) };
        case 'topics':
          return { path, label: formatTopicName(segment) };
      }
    }

    return { path, label: segment };
  }

  let parts = $derived(page.url.pathname.split('/').filter(Boolean).slice(1));

  let crumbs = $derived(parts.map((segment, index) => formatPathSegment(segment, index, parts)));
</script>

<div class="flex items-center">
  <a href={resolve(typedRoute('/dashboard/overview'))}>
    <Icon name="home" class="dark:fill-shade-d900 dark:stroke-white mr-2" />
  </a>

  <span class="flex mr-2">
    <span class="text-shade-l800">/</span>
  </span>

  {#each crumbs as { path, label }, idx (idx)}
    {@const isLast = idx === crumbs.length - 1}

    {#if isLast}
      <span class="flex ml-1">
        <span class="text-color font-semibold">{label}</span>
      </span>
    {:else}
      <a href={resolve(path)} class="flex ml-1">
        <span class={twMerge('font-medium text-gray-600 dark:text-shade-l800')}>{label}</span>
        <span class="font-medium text-shade-l800 w-[10px] flex items-center justify-center ml-1"
          >/</span
        >
      </a>
    {/if}
  {/each}
</div>
