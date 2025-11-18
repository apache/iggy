<script lang="ts">
  import Icon from './Icon.svelte';
  import type { iconType } from './Icon.svelte';
  import { page } from '$app/state';
  import { twMerge } from 'tailwind-merge';
  import { tooltip } from '$lib/actions/tooltip';
  import { typedRoute } from '$lib/types/appRoutes';
  import LogoType from '$lib/components/Logo/LogoType.svelte';
  import LogoMark from '$lib/components/Logo/LogoMark.svelte';
  import { resolve } from '$app/paths';

  let navItems = $derived([
    {
      name: 'Overview',
      icon: 'home',
      href: typedRoute('/dashboard/overview'),
      active: page.url.pathname.includes(typedRoute('/dashboard/overview'))
    },
    {
      name: 'Streams',
      icon: 'stream',
      href: typedRoute('/dashboard/streams'),
      active: page.url.pathname.includes(typedRoute('/dashboard/streams'))
    },
    // {
    //   name: 'Clients',
    //   icon: 'clients',
    //   href: typedRoute('/dashboard/clients'),
    //   active: page.url.pathname.includes(typedRoute('/dashboard/clients'))
    // },
    // {
    //   name: 'Logs',
    //   icon: 'logs',
    //   href: typedRoute('/dashboard/logs'),
    //   active: page.url.pathname.includes(typedRoute('/dashboard/logs'))
    // },
    {
      name: 'Settings',
      icon: 'settings',
      href: typedRoute('/dashboard/settings/webUI'),
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
            href={resolve(href)}
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
