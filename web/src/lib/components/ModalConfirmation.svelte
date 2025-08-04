<script lang="ts">
  import { fade, slide } from 'svelte/transition';
  import Button from './Button.svelte';
  import { createEventDispatcher } from 'svelte';
  import Input from './Input.svelte';
  import Icon from './Icon.svelte';
  import { Keys } from '$lib/utils/constants/keys';

  interface Props {
    open: boolean;
    retypeText: string;
    deleteButtonTitle: string;
    message?: import('svelte').Snippet;
  }

  let {
    open,
    retypeText,
    deleteButtonTitle,
    message
  }: Props = $props();

  let retypedText = $state('');

  const dispatch = createEventDispatcher<{ result: boolean }>();
</script>

{#if open}
  <div
    transition:fade={{ duration: 100 }}
    class="absolute z-40 bg-black opacity-40 backdrop-blur-xs rounded-2xl inset-0"
></div>
  <div
    transition:fade={{ duration: 100 }}
    onclick={() => dispatch('result', false)}
    onkeydown={(e) => {
      if (e.key === Keys.ENTER || e.key === Keys.SPACE) {
        e.preventDefault();
        dispatch('result', false);
      }
    }}
    role="button"
    tabindex="0"
    aria-label="Close confirmation dialog"
    class="absolute z-40 backdrop-blur-xs rounded-2xl inset-3"
></div>
  <div
    transition:slide={{ duration: 300 }}
    class="absolute bottom-0 left-0 right-0 flex flex-col z-50 items-center bg-shadeL100 dark:bg-shadeD700 rounded-tl-2xl rounded-tr-2xl"
  >
    <div class="p-5 pt-10 flex flex-col items-center border-b relative text-color">
      <Button
        variant="rounded"
        onclick={() => dispatch('result', false)}
        class="absolute top-3 right-3 p-2"
      >
        <Icon name="close" strokeWidth={2.3} />
      </Button>

      <h2 class=" font-semibold text-xl">Are you sure?</h2>
      {#if message}
        <span class="text-sm block w-full text-center my-1">
          {@render message?.()}
        </span>
      {/if}
    </div>
    <div class="mt-5 w-full px-5 mb-5 gap-4 flex flex-col text-color">
      <span class="block text-sm"
        >To confirm type "<span class="italic">{retypeText}</span>" in the box below</span
      >
      <Input name="retypeText" bind:value={retypedText} />

      <div class="w-full">
        <Button
          variant="containedRed"
          onclick={() => dispatch('result', true)}
          disabled={retypedText !== retypeText}
          class="w-full">{deleteButtonTitle}</Button
        >
      </div>
    </div>
  </div>
{/if}

<!-- <div class="flex gap-2 p-5 py-10 w-full">
  <Button variant="containedRed" class="flex-1" onclick={() => dispatch('result', true)}
    >Yes</Button
  >
  <Button variant="text" class="flex-1" onclick={() => dispatch('result', false)}>No</Button>
</div> -->
