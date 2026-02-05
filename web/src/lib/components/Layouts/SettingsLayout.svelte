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
  import { twMerge } from 'tailwind-merge';
  import type { iconType } from '$lib/components/Icon.svelte';
  import Icon from '$lib/components/Icon.svelte';

  import { typedRoute } from '$lib/types/appRoutes';
  import { page } from '$app/state';
  import { resolve } from '$app/paths';
  interface Props {
    actions?: import('svelte').Snippet;
    children?: import('svelte').Snippet;
  }

  let { actions, children }: Props = $props();

  type Tabs = 'server' | 'users' | 'webUI' | 'terminal';

  let activeTab = $derived(page.url.pathname.split('/').slice(-1)[0]);

  const tabs = [
    {
      tab: 'server',
      icon: 'adjustments',
      name: 'Server',
      path: typedRoute('/dashboard/settings/server')
    },
    {
      tab: 'webUI',
      icon: 'settings',
      name: 'Web UI',
      path: typedRoute('/dashboard/settings/webUI')
    },
    {
      tab: 'users',
      icon: 'usersGroup',
      name: 'Users',
      path: typedRoute('/dashboard/settings/users')
    }
  ] satisfies { tab: Tabs; name: string; icon: iconType; path: string }[];
</script>

<div class="flex justify-between items-center px-10">
  <h1 class="font-semibold text-3xl text-color my-10">Settings</h1>

  {@render actions?.()}
</div>

<div class="flex gap-12 border-b px-10">
  {#each tabs as { icon, name, path }, idx (idx)}
    {@const isActive = activeTab === path.split('/').slice(-1)[0]}
    <a
      href={resolve(path)}
      class={twMerge('pb-3 relative group flex items-center  justify-start gap-2 text-color')}
    >
      <Icon name={icon} class="w-[15px] h-[15px]" />

      <span>
        {name}
      </span>

      <div
        class={twMerge(
          'absolute left-0 right-0 top-full h-[2px] -translate-y-full rounded-tl-md rounded-tr-md transition-colors duration-200',
          isActive
            ? 'dark:bg-white bg-black'
            : 'group-hover:bg-shade-l600 dark:group-hover:bg-shade-d300'
        )}
      ></div>
    </a>
  {/each}
</div>

{@render children?.()}
