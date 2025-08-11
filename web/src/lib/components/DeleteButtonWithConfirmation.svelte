<script lang="ts">
  import { run } from 'svelte/legacy';

  import { tooltip } from '$lib/actions/tooltip';
  import { fade } from 'svelte/transition';
  import Button from './Button.svelte';

  let isTooltipOpen = $state(false);
  let wrapperRef: HTMLDivElement | null = $state<HTMLDivElement | null>(null);

  let counter = $state(7);
  let interval: number | undefined = undefined;

  function startCountingDown() {
    if (interval) return;
    interval = setInterval(() => {
      counter -= 1;
      if (counter == 0) clearInterval(interval);
    }, 1000);
  }

  function clearCounterState() {
    clearInterval(interval);
    interval = undefined;
    counter = 7;
  }

  run(() => {
    isTooltipOpen ? startCountingDown() : clearCounterState();
  });
</script>

<div bind:this={wrapperRef} use:tooltip={{ placement: 'top' }}>
  <Button variant="containedRed" onclick={() => (isTooltipOpen = true)}>Delete</Button>

  <div class="tooltip">
    <div class="flex flex-col gap-4 items-center justify-center p-2">
      <span>Are you sure? </span>
      <div class="flex flex-row gap-2">
        <Button variant="text" type="button" onclick={() => (isTooltipOpen = false)} size="sm"
          >No</Button
        >
        <Button
          variant="containedRed"
          type="submit"
          size="sm"
          class="w-[45px] {counter > 0 ? 'transition-none' : 'transition-all'}"
          disabled={counter > 0}
        >
          {#if counter > 0}
            {#key counter}
              <span class="text-black" in:fade={{ duration: 200 }}>
                {counter}
              </span>
            {/key}
          {:else}
            Yes
          {/if}
        </Button>
      </div>
    </div>
  </div>
</div>
