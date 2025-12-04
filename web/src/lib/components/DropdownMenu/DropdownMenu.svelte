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
  import { tooltip } from '$lib/actions/tooltip';
  import type { Placement } from '@floating-ui/dom';

  interface Props {
    class?: string;
    placement: Placement;
    trigger?: import('svelte').Snippet;
    children?: import('svelte').Snippet<[any]>;
  }

  let { class: className = '', placement, trigger, children }: Props = $props();

  let tooltipRef = $state<HTMLDivElement | null>(null);

  const closeTooltip = () => tooltipRef?.dispatchEvent(new Event('closeTooltip'));
</script>

<div class={className} use:tooltip={{ placement, clickable: true }}>
  {@render trigger?.()}

  <div class="tooltip" bind:this={tooltipRef}>
    <div class="z-50">
      {@render children?.({ close: closeTooltip })}
    </div>
  </div>
</div>

<!-- {#each itemGroups as group}
  <div class="px-1 py-1">
    {#each group as { action, icon, className, label }}
      <button
        onclick={() => {
          if (action) {
            action(() => tooltipRef.dispatchEvent(new Event('closeTooltip')));
          }
        }}
        class={twMerge(
          ' grid grid-cols-[20px_1fr] gap-x-1 rounded-md items-center w-full px-2 py-2 text-sm text-color cursor-default',
          action && 'hoverable cursor-pointer'
        )}
      >
        <span>
          <Icon name={icon} class="w-[17px] h-[17px] stroke-white" />
        </span>
        <span class={twMerge('text-left', className)}>
          {label}
        </span>
      </button>
    {/each}
  </div>
{/each} -->
