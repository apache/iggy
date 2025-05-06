<script lang="ts" module>
  import { flip } from 'svelte/animate';
  import { writable } from 'svelte/store';
  import { fly, fade } from 'svelte/transition';
  import { cubicOut } from 'svelte/easing';
  import { twMerge } from 'tailwind-merge';
  import type { iconType } from './Icon.svelte';
  import Icon from './Icon.svelte';

  type ToastType = 'success' | 'error' | 'info';

  type Toast = {
    id: number;
    duration: number;
    type: ToastType;
    description: string;
    remove: () => void;
  };

  const toastsStore = writable<Toast[]>([]);

  export function showToast({
    description,
    duration = 2200,
    type
  }: {
    duration?: number;
    type: ToastType;

    description: string;
  }): Promise<void> {
    const toastId = Date.now();
    const obj = {
      id: toastId,
      duration,
      description,
      type,
      remove: () => toastsStore.update((v) => v.filter((i) => i.id !== toastId))
    };

    toastsStore.update((v) => [...v, obj]);

    // 0 seconds duration means toast will never disappear
    return new Promise((res, rej) => {
      if (duration > 0) {
        setTimeout(() => {
          obj.remove();
          res();
        }, duration);
      }
    });
  }

  const icons = {
    success: 'checkCircle',
    error: 'alertCircle',
    info: 'infoCircle'
  } satisfies Record<ToastType, iconType>;

  const title = {
    success: 'Success',
    error: 'Something went wrong'
  };
</script>

<div class="fixed p-2 pointer-events-none bottom-5 right-5 z-50">
  <ul class="flex flex-col gap-2">
    {#each $toastsStore as { id, remove, type, description } (id)}
      <li
        animate:flip={{ duration: 250, easing: cubicOut }}
        in:fly={{ duration: 300, x: '70%', easing: cubicOut }}
        out:fade={{ duration: 300, easing: cubicOut }}
      >
        <div
          role="alert"
          aria-live="assertive"
          aria-atomic="true"
          class={twMerge(
            'p-3 flex gap-4 items-start justify-start shadow-md w-[320px] text-base text-color dark:bg-shadeD500 relative border border-l pointer-events-auto',
            type === 'success' && 'border-green-600 fill-green-500 bg-green-100 border-l-[3px]',
            type === 'error' && 'border-red-600',
            type === 'info' && ''
          )}
        >
          <Icon
            class={twMerge(type === 'success' && 'fill-green-500 ', 'stroke-green-100')}
            name={icons[type]}
          />

          <button class="rounded-full p-1 absolute top-3 right-3" onclick={remove}>
            <Icon name="close" class="w-[16px] h-[16px]" />
          </button>

          <div class="flex flex-col">
            <span>
              {#if type !== 'info'}
                <span class="font-semibold">{title[type]}</span>
              {/if}
            </span>

            <span class="text-sm">{description}</span>
          </div>
        </div>
      </li>
    {/each}
  </ul>
</div>
