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

<script lang="ts" generics="T extends string">
  interface Props {
    options: { value: T; label: string }[];
    value: T;
    onchange?: (_value: T) => void;
  }

  let { options, value = $bindable(), onchange }: Props = $props();

  function select(v: T) {
    value = v;
    onchange?.(v);
  }
</script>

<div class="flex rounded-md ring-1 ring-gray-300 dark:ring-gray-500 overflow-hidden w-fit">
  {#each options as option (option.value)}
    <button
      type="button"
      onclick={() => select(option.value)}
      class="px-3 h-[32px] text-sm transition-colors text-color"
      class:bg-gray-200={value === option.value}
      class:dark:bg-shade-d200={value === option.value}
      class:font-medium={value === option.value}
    >
      {option.label}
    </button>
  {/each}
</div>
