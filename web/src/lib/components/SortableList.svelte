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

<script lang="ts" module>
  type ListItem = {
    id: string | number;
  };
  type Ordering<T> = { key: keyof T; asc: boolean } | { key: undefined; asc: undefined };

  function orderData<T>(data: T[], key: keyof T, asc: boolean) {
    return data.sort((a, b) => {
      if (a[key] > b[key]) return asc ? 1 : -1;
      else if (a[key] < b[key]) return asc ? -1 : 1;
      return 0;
    });
  }
</script>

<script lang="ts" generics="T extends ListItem">
  import { run } from 'svelte/legacy';

  import { asConst } from '$lib/utils/asConst';

  import { onNavigate } from '$app/navigation';

  import { slide } from 'svelte/transition';

  import Icon from './Icon.svelte';

  import { twMerge } from 'tailwind-merge';
  import { noTypeCheck } from '$lib/utils/noTypeCheck';

  let animationEnabled = $state(true);
  let isAnimating = $state(false);
  let timeout: ReturnType<typeof setTimeout>;

  onNavigate(() => {
    animationEnabled = false;

    return () => {
      if (timeout) clearTimeout(timeout);
      timeout = setTimeout(() => {
        animationEnabled = true;
      }, 800);
    };
  });

  interface Props {
    data: T[];
    emptyDataMessage: string;
    colNames: Partial<Record<keyof T, string | undefined>>;
    rowClass: string;
    hrefBuilder?: ((_item: T) => string) | undefined;
    onclickAction?: ((_index: number) => any) | undefined;
    ariaRoleDescription?: string | undefined;
    children?: import('svelte').Snippet<[any]>;
  }

  let {
    data,
    emptyDataMessage,
    colNames,
    rowClass,
    hrefBuilder = undefined,
    onclickAction = undefined,
    ariaRoleDescription = undefined,
    children
  }: Props = $props();

  let ordering: Ordering<T> = $state({
    key: undefined,
    asc: undefined
  });

  let columns = $derived(
    Object.keys(colNames)
      .filter((k) => colNames[k as string])
      .map((key) => ({
        columnDisplayedName: colNames[key as keyof T],
        columnName: key
      })) as { columnDisplayedName: string; columnName: keyof T }[]
  );

  let orderedData = $derived(ordering.key ? orderData(data, ordering.key, ordering.asc!) : data);

  let isBodyOverflowing = $state(false);
  let bodyElem: HTMLDivElement | null = $state(null);
  run(() => {
    if (bodyElem && bodyElem.scrollHeight > bodyElem.offsetHeight) {
      isBodyOverflowing = true;
    }
  });
</script>

<div class="flex-1 overflow-auto">
  <!-- header -->
  <div
    class={twMerge(
      'flex h-[60px] min-h-[60px] bg-shade-l300 dark:bg-shade-d400 min-w-[1300px]',
      isBodyOverflowing && 'pr-5'
    )}
  >
    <div class="w-full {rowClass}">
      {#each columns as { columnName, columnDisplayedName } (columnName)}
        <button
          onclick={() =>
            (ordering = {
              key: columnName,
              asc: ordering.key !== columnName ? true : !ordering.asc
            })}
          class="flex items-center justify-between px-5 transition-colors outline-hidden hover:cursor-pointer hover:bg-shade-l400 dark:hover:bg-shade-d200 dark:text-white focus:outline-hidden focus-visible:ring-3 ring-inset ring-blue-600/60"
        >
          <span>
            {columnDisplayedName}
          </span>

          <div class="flex flex-col">
            {#each asConst(['caretUp', 'caretDown']) as icon (icon)}
              <Icon
                name={icon}
                class={twMerge(
                  'fill-shade-l800 stroke-shade-l800 dark:fill-shade-l900 dark:stroke-shade-l900 w-[18px] h-fit -mb-[4px] -my-[2px]',
                  ordering.key === columnName &&
                    ordering.asc &&
                    icon === 'caretUp' &&
                    ' stroke-black  fill-black dark:stroke-white dark:fill-white',
                  ordering.key === columnName &&
                    !ordering.asc &&
                    icon === 'caretDown' &&
                    ' stroke-black fill-black dark:stroke-white dark:fill-white'
                )}
              />
            {/each}
          </div>
        </button>
      {/each}
    </div>
  </div>

  <!-- body -->
  <div class="min-w-[1300px] h-[calc(100%-60px)] pb-3 overflow-auto" bind:this={bodyElem}>
    {#if data.length === 0 && !isAnimating}
      <div class="flex items-center justify-center text-lg text-gray-400 mt-14">
        <em>{emptyDataMessage}</em>
      </div>
    {/if}

    {#each orderedData as item, index (index)}
      <svelte:element
        this={hrefBuilder ? 'a' : 'div'}
        href={hrefBuilder && hrefBuilder(item)}
        role="button"
        tabindex={index}
        aria-roledescription={ariaRoleDescription}
        onclick={() => onclickAction?.(index)}
        onintrostart={noTypeCheck((e) => {
          if (!animationEnabled) return;
          e.target.style.backgroundColor = 'rgb(74, 222, 128)';
          isAnimating = true;
        })}
        onintroend={(e) => {
          setTimeout(() => {
            if (!animationEnabled) return;
            e.target.style = '';
          }, 800);
        }}
        onoutrostart={(e) => {
          if (!animationEnabled) return;
          e.target.style.backgroundColor = 'rgb(239 68 68)';
          isAnimating = true;
        }}
        onoutroend={() => {
          isAnimating = false;
        }}
        in:slide={{ duration: animationEnabled ? 300 : 0 }}
        out:slide={{ duration: animationEnabled ? 300 : 0, delay: animationEnabled ? 800 : 0 }}
        class={twMerge(
          'flex flex-row h-[65px] border-b hover:cursor-pointer hoverable dark:text-white'
        )}
      >
        <div class="flex items-center w-full {rowClass}">
          {#if children}{@render children({ item })}{:else}
            {#each columns as { columnName }, idx (idx)}
              <div class={twMerge('px-5')}>
                <span class={columnName === 'id' ? 'font-semibold' : ''}>
                  {item[columnName]}
                </span>
              </div>
            {/each}
          {/if}
        </div>
      </svelte:element>
    {/each}
  </div>
</div>
