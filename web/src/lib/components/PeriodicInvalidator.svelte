<script lang="ts" module>
  import { browser } from '$app/environment';
  import { invalidateAll } from '$app/navigation';
  import { onMount } from 'svelte';
  import { writable } from 'svelte/store';
  import Button from './Button.svelte';
  import { twMerge } from 'tailwind-merge';
  import Icon from './Icon.svelte';

  const isInvalidating = writable(false);
  const isInvalidatingClampMin = writable(false);
  export const invalidateIntervalDuration = writable(4000);

  export async function customInvalidateAll() {
    if (!browser) return;
    isInvalidating.set(true);
    await invalidateAll();
    isInvalidating.set(false);
  }
</script>

<script lang="ts">
  import { run } from 'svelte/legacy';

  let timeout: number | undefined = $state();

  run(() => {
    if ($isInvalidating) {
      if (timeout) clearTimeout(timeout);
      $isInvalidatingClampMin = true;

      timeout = setTimeout(() => {
        if (!$isInvalidating) $isInvalidatingClampMin = false;
      }, 320);
    }
  });
  let invalidateInterval: number | undefined = $state();
  run(() => {
    if (invalidateInterval) clearInterval(invalidateInterval);
    invalidateInterval = setInterval(() => {
      customInvalidateAll();
    }, $invalidateIntervalDuration);
  });

  onMount(() => {
    return () => {
      clearInterval(invalidateInterval);
      clearTimeout(timeout);
    };
  });
</script>

<Button
  variant="rounded"
  onclick={customInvalidateAll}
>
  {#snippet children()}
    <div class={twMerge($isInvalidatingClampMin && 'spin')}>
      <Icon name="refresh" class="dark:text-white" />
    </div>
  {/snippet}
  {#snippet tooltip()}
    <div>Refresh</div>
  {/snippet}
</Button>

<style lang="postcss">
  .spin {
    animation: spin 0.3s linear infinite;
  }

  @keyframes spin {
    100% {
      transform: rotate(-180deg);
    }
  }
</style>
