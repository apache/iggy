<script lang="ts">
  import { createBubbler, handlers } from 'svelte/legacy';

  const bubble = createBubbler();
  import { twMerge } from 'tailwind-merge';

  let isMouseDown = $state(false);
  interface Props {
    checked?: boolean | undefined;
    value: string;
    id?: string;
    name?: string | undefined;
    bindGroup?: string[] | undefined;
    disabled?: boolean;
    onclick?: (e: Event) => void;
  }

  let {
    checked = $bindable(undefined),
    value,
    id = '',
    name = undefined,
    bindGroup = $bindable(undefined),
    disabled = false,
    onclick
  }: Props = $props();

  function onChange(e: Event) {
    const { value: checkboxValue, checked: checkboxChecked } = e.target as HTMLInputElement;
    if (bindGroup) {
      if (checkboxChecked) {
        bindGroup = [...bindGroup, checkboxValue];
      } else {
        bindGroup = bindGroup.filter((item) => item !== value);
        checked = checkboxChecked;
      }
    } else {
      checked = checkboxChecked;
    }
  }
</script>

<div class="relative w-8 h-8">
  <input
    type="checkbox"
    checked={bindGroup?.includes(value) || checked}
    {name}
    {value}
    {id}
    {disabled}
    onchange={handlers(onChange, bubble('change'))}
    onclick={onclick}
    onmousedown={() => (isMouseDown = true)}
    onmouseup={() => (isMouseDown = false)}
    onmouseleave={() => (isMouseDown = false)}
    onmouseout={() => (isMouseDown = false)}
    onblur={() => (isMouseDown = false)}
  />

  <div
    role="checkbox"
    tabindex="0"
    aria-checked={checked}
    class={twMerge(
      'w-[18px] h-[18px] absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 border-neutral-300 border-2 rounded transition-all pointer-events-none',
      isMouseDown && 'scale-90'
    )}
></div>

  <svg
    class="w-[18px] h-[18px] pointer-events-none transition-all scale-75 opacity-0 absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2"
    viewBox="0 0 24 24"
    fill="none"
    xmlns="http://www.w3.org/2000/svg"
  >
    <path
      id="Vector"
      d="M6 12L10.2426 16.2426L18.727 7.75732"
      stroke-width="2.4"
      stroke-linecap="round"
      stroke-linejoin="round"
    />
  </svg>
</div>

<style lang="postcss">
  input[type='checkbox'] {
    @apply cursor-pointer appearance-none m-0 absolute inset-0;
  }

  input[type='checkbox'] + div {
    @apply bg-shadeL100;
  }

  input[type='checkbox']:hover:not(:checked):not(:disabled) + div {
    @apply border-neutral-400;
  }

  input[type='checkbox']:checked + div {
    @apply bg-[#32AD84];
  }

  input[type='checkbox']:checked + div + svg {
    @apply opacity-100 stroke-white;
  }

  input[type='checkbox']:checked:disabled + div {
    @apply opacity-50;
  }

  input[type='checkbox']:disabled {
    @apply cursor-not-allowed;
  }
</style>
