<script lang="ts">
  import { twMerge } from 'tailwind-merge';
  import Icon from '../Icon.svelte';
  import type { TransitionConfig } from 'svelte/transition';
  import { quintOut } from 'svelte/easing';
  import Button from '../Button.svelte';
  import type { CloseModalFn } from '$lib/types/utilTypes';

  interface Props {
    closeModal: CloseModalFn;
    title?: string;
    titleSuffix?: import('svelte').Snippet;
    children?: import('svelte').Snippet;
    [key: string]: any
  }

  let {
    titleSuffix,
    children,
    ...rest_1
  }: Props = $props();

  function modalTransition(node: Element): TransitionConfig {
    const style = getComputedStyle(node);
    const transform = style.transform === 'none' ? '' : style.transform;

    const scaleStart = 0.94;

    //t= 0..1 on enter and 1..0 on unmount
    //u = 1..0 on enter and 0..1 on unmount

    const ss = 1 - scaleStart;

    return {
      duration: 250,
      easing: quintOut,
      css: (t, u) => `transform: ${transform} scale(${1 - u * ss});
                          opacity: ${t}`
    };
  }



  const { class: className, closeModal, title, ...rest } = rest_1 as Props;
</script>

<div
  {...rest}
  transition:modalTransition
  class={twMerge(
    'fixed left-1/2 top-1/2  shadow-lg -translate-x-1/2 -translate-y-1/2 rounded-2xl bg-shadeL100 dark:bg-shadeD700 z-[600]  max-h-[90vh] overflow-auto min-w-[min(90vw,400px)] p-7 pb-5 flex flex-col',
    className
  )}
>
  <div class="h-[15%]">
    <Button variant="rounded" onclick={() => closeModal()} class="absolute p-2 top-5 right-5">
      <Icon name="close" strokeWidth={2.3} />
    </Button>

    {#if title}
      <div class="flex items-center gap-2">
        <h2 class="text-xl font-semibold text-color mb-7">{title}</h2>
        {@render titleSuffix?.()}
      </div>
    {/if}
  </div>

  {@render children?.()}
</div>
