<script lang="ts" module>
  type Ordering<T> = { key: keyof T; asc: boolean } | { key: undefined; asc: undefined };

  function orderData<T>(data: T[], key: keyof T, asc: boolean) {
    return data.sort((a, b) => {
      if (a[key] > b[key]) return asc ? 1 : -1;
      else if (a[key] < b[key]) return asc ? -1 : 1;
      return 0;
    });
  }
</script>

<script lang="ts">
  import { run } from 'svelte/legacy';

  import { asConst } from '$lib/utils/asConst';

  import Icon from './Icon.svelte';
  import { twMerge } from 'tailwind-merge';

  type T = $$Generic<{ id: string | number }>;
  type Column =
    | { label: string; slot?: undefined; key?: undefined; sortable?: false }
    | { slot: true; sortable?: undefined; key?: undefined; label?: undefined }
    | { label: string; slot?: false; key: keyof T; sortable: boolean };

  interface Props {
    data: Array<T>;
    columns: Array<Column>;
    emptyDataMessage: string;
    gridColsClass: string;
    header?: import('svelte').Snippet<[any]>;
    children?: import('svelte').Snippet<[any]>;
  }

  let {
    data,
    columns,
    emptyDataMessage,
    gridColsClass,
    header,
    children
  }: Props = $props();

  let ordering: Ordering<T> = $state({
    key: 'id',
    asc: true
  });

  let orderedData = $derived(ordering.key ? orderData(data, ordering.key, ordering.asc!) : data);

  let isBodyOverflowing = $state(false);
  let bodyElem: HTMLDivElement | null = $state(null);
  run(() => {
    if (bodyElem && bodyElem.scrollHeight > bodyElem.offsetHeight) {
      isBodyOverflowing = true;
    }
  });
</script>

<div class="overflow-auto flex-1">
  <!-- header -->
  <div
    class={twMerge(
      'flex h-[60px] min-h-[60px] bg-shadeL300 dark:bg-shadeD400 min-w-[1300px]',
      isBodyOverflowing && 'pr-5'
    )}
  >
    <div class="w-full grid {gridColsClass}">
      {#each columns as { label, slot, key, sortable }, idx (idx)}
        {#if slot}
          {@render header?.({ index: idx, })}
        {:else}
          <button
            onclick={() => {
              if (!sortable || !key) return;
              ordering = {
                key: key,
                asc: ordering.key !== key ? true : !ordering.asc
              };
            }}
            class="flex items-center px-5 hover:cursor-pointer hover:bg-shadeL400 dark:hover:bg-shadeD200 dark:text-white justify-between outline-none focus:outline-none focus-visible:ring ring-inset ring-blue-600/60 transition-colors"
          >
            <span>
              {label}
            </span>

            {#if sortable}
              <div class="flex flex-col">
                {#each asConst(['caretUp', 'caretDown']) as icon}
                  <Icon
                    name={icon}
                    class={twMerge(
                      'fill-shadeL800 stroke-shadeL800 dark:fill-shadeL900 dark:stroke-shadeL900 w-[18px] h-fit -mb-[4px] -my-[2px]',
                      ordering.key === key &&
                        ordering.asc &&
                        icon === 'caretUp' &&
                        ' stroke-black  fill-black dark:stroke-white dark:fill-white',
                      ordering.key === key &&
                        !ordering.asc &&
                        icon === 'caretDown' &&
                        ' stroke-black fill-black dark:stroke-white dark:fill-white'
                    )}
                  />
                {/each}
              </div>
            {/if}
          </button>
        {/if}
      {/each}
    </div>
  </div>

  <!-- body -->
  <div class="min-w-[1300px] h-[calc(100%-60px)] pb-3 overflow-auto" bind:this={bodyElem}>
    {#if data.length === 0}
      <div class="flex items-center justify-center text-gray-400 mt-14 text-lg">
        <em>{emptyDataMessage}</em>
      </div>
    {/if}

    {#each orderedData as item (item.id)}
      {@render children?.({ item, baseClass: `grid ${gridColsClass} items-center h-[65px] border-b hoverable dark:text-white`, })}
    {/each}
  </div>
</div>
