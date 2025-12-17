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
  import Icon from './Icon.svelte';
  import type { iconType } from './Icon.svelte';
  import { page } from '$app/state';
  import { resolve } from '$app/paths';
  import { twMerge } from 'tailwind-merge';
  import { tooltip } from '$lib/actions/tooltip';
  import { typedRoute } from '$lib/types/appRoutes';
  import LogoType from '$lib/components/Logo/LogoType.svelte';
  import LogoMark from '$lib/components/Logo/LogoMark.svelte';
  let navItems = $derived([
    {
      name: 'Overview',
      icon: 'home',
      href: resolve(typedRoute('/dashboard/overview')),
      active: page.url.pathname.includes(typedRoute('/dashboard/overview'))
    },
    {
      name: 'Streams',
      icon: 'stream',
      href: resolve(typedRoute('/dashboard/streams')),
      active: page.url.pathname.includes(typedRoute('/dashboard/streams'))
    },
    // {
    //   name: 'Clients',
    //   icon: 'clients',
    //   href: resolve(typedRoute('/dashboard/clients')),
    //   active: page.url.pathname.includes(typedRoute('/dashboard/clients'))
    // },
    // {
    //   name: 'Logs',
    //   icon: 'logs',
    //   href: resolve(typedRoute('/dashboard/logs')),
    //   active: page.url.pathname.includes(typedRoute('/dashboard/logs'))
    // },
    {
      name: 'Settings',
      icon: 'settings',
      href: resolve(typedRoute('/dashboard/settings/webUI')),
      active: page.url.pathname.includes('/dashboard/settings')
    }
  ] satisfies { name: string; icon: iconType; href: string; active: boolean }[]);
</script>

<nav
  class="fixed z-10 left-0 top-0 bottom-0 min-w-[90px] max-w-[90px] pb-7 pt-4 border-r flex flex-col items-center bg-shade-l300 dark:bg-shade-d1000"
>
  <a
    href={resolve(typedRoute('/dashboard/overview'))}
    class="flex flex-col items-center gap-5 mb-5"
  >
    <LogoType class="w-[51px] h-[28px] pointer-events-none" />
    <LogoMark class="w-[50px] h-[45px]" />
  </a>

  <ul class="flex flex-col gap-7">
    {#each navItems as { name, icon, href, active } (name + href)}
      <li>
        <div use:tooltip={{ placement: 'right' }}>
          <a
            {href}
            data-trigger
            class={twMerge(
              'p-2 block rounded-xl transition-colors  ring-2 ring-transparent',
              active && 'ring-black dark:ring-white',
              !active && 'hover:bg-shade-l500 dark:hover:bg-shade-d300'
            )}
          >
            <Icon name={icon} class="w-[27px] h-[27px] text-black dark:text-white" />
          </a>
          <div class="tooltip">
            {name}
          </div>
        </div>
      </li>
    {/each}
  </ul>
</nav>
