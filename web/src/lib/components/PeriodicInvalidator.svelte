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

  let timeout: ReturnType<typeof setTimeout> | undefined = $state();

  run(() => {
    if ($isInvalidating) {
      if (timeout) clearTimeout(timeout);
      $isInvalidatingClampMin = true;

      timeout = setTimeout(() => {
        if (!$isInvalidating) $isInvalidatingClampMin = false;
      }, 320);
    }
  });
  let invalidateInterval: ReturnType<typeof setInterval> | undefined = $state();
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

<Button variant="rounded" onclick={customInvalidateAll}>
  <div class={twMerge($isInvalidatingClampMin && 'spin')}>
    <Icon name="refresh" class="dark:text-white" />
  </div>
  {#snippet tooltip()}
    <div>Refresh</div>
  {/snippet}
</Button>

<style lang="postcss">
  @reference "../../styles/app.css";
  .spin {
    animation: spin 0.3s linear infinite;
  }

  @keyframes spin {
    100% {
      transform: rotate(-180deg);
    }
  }
</style>
