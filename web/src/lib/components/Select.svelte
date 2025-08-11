<script lang="ts">
  import { createBubbler } from 'svelte/legacy';

  const bubble = createBubbler();
  import type { HTMLInputTypeAttribute } from 'svelte/elements';
  import { twMerge } from 'tailwind-merge';

  

  interface Props {
    errorMessage?: string;
    id?: string;
    label?: string;
    name: string;
    options: Array<string>;
    type?: HTMLInputTypeAttribute;
    value: string | number;
    prefix?: import('svelte').Snippet;
    suffix?: import('svelte').Snippet;
    [key: string]: any
  }

  let {
    errorMessage = undefined,
    id = crypto.randomUUID(),
    label = undefined,
    name,
    options,
    value = $bindable(),
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
    <label for={id} class="ml-1 text-sm text-color">
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
      <div class="absolute flex items-center justify-center -translate-y-1/2 left-2 top-1/2">
        {@render prefix?.()}
      </div>
    {/if}

    <select bind:value oninput={bubble('input')} {...inputProps}>
      {#each options as option}
        <option class="dark:bg-shadeD400 dark:text-white bg-white text-black">{option}</option>
      {/each}
    </select>

    {#if suffix}
      <div class="absolute flex items-center justify-center -translate-y-1/2 right-2 top-1/2">
        {@render suffix?.()}
      </div>
    {/if}
  </div>

  {#if errorMessage}
    <span class="ml-1 text-xs font-medium text-red-600">
      {errorMessage}
    </span>
  {/if}
</div>
