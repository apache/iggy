<script lang="ts">
  import { createBubbler } from 'svelte/legacy';

  const bubble = createBubbler();
  import type { HTMLInputTypeAttribute } from 'svelte/elements';
  import { twMerge } from 'tailwind-merge';



  interface Props {
    label?: string;
    id?: string;
    name: string;
    errorMessage?: string;
    value: string | number;
    type?: HTMLInputTypeAttribute;
    prefix?: import('svelte').Snippet;
    suffix?: import('svelte').Snippet;
    [key: string]: any
  }

  let {
    label = undefined,
    id = crypto.randomUUID(),
    name,
    errorMessage = undefined,
    value = $bindable(),
    type = 'text',
    prefix,
    suffix,
    ...rest
  }: Props = $props();

  const inputProps = {
    class: twMerge(
      'w-full px-4 h-full rounded-lg outline-none bg-transparent text-color',
      prefix && 'pl-9',
      suffix && 'pr-12'
    ),
    id,
    name,
    ...rest
  };
</script>

<div class="flex flex-col gap-2">
  {#if label}
    <label for={id} class="text-sm ml-1 text-color">
      {label}
    </label>
  {/if}

  <div
    class={twMerge(
      'rounded-md dark:bg-shadeD400  ring-1 ring-gray-300 dark:ring-gray-500 flex items-center h-[40px] text-color relative focus-within:ring-2 focus-within:ring-gray-400 transition group',
      errorMessage && '!ring-red-600 ring-2 '
    )}
  >
    {#if prefix}
      <div class="flex items-center justify-center absolute left-2 top-1/2 -translate-y-1/2">
        {@render prefix?.()}
      </div>
    {/if}

    {#if type === 'text'}
      <input bind:value type="text" oninput={bubble('input')} {...inputProps} />
    {:else if type === 'password'}
      <input bind:value type="password" oninput={bubble('input')} {...inputProps} />
    {:else if type === 'number'}
      <input bind:value type="number" oninput={bubble('input')} {...inputProps} />
    {/if}

    {#if suffix}
      <div class="flex items-center justify-center absolute right-2 top-1/2 -translate-y-1/2">
        {@render suffix?.()}
      </div>
    {/if}
  </div>

  {#if errorMessage}
    <span class="text-red-600 text-xs font-medium ml-1">
      {errorMessage}
    </span>
  {/if}
</div>
