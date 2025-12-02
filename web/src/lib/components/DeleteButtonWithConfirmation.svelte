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

<script lang="ts">
  import { run } from 'svelte/legacy';

  import { tooltip } from '$lib/actions/tooltip';
  import { fade } from 'svelte/transition';
  import Button from './Button.svelte';

  let isTooltipOpen = $state(false);
  let wrapperRef: HTMLDivElement | null = $state<HTMLDivElement | null>(null);

  let counter = $state(7);
  let interval: ReturnType<typeof setInterval> | undefined = undefined;

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
