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
  interface Props {
    data: any;
  }

  let { data }: Props = $props();

  let statsValues = $derived(
    Object.values(data.stats).map((val) => val as { name: string; value: string })
  );
</script>

<div class="h-full overflow-auto p-10">
  <div class="p-5 w-fit mx-auto rounded-md border dark:border">
    <h1 class="font-semibold text-xl text-color mb-5">System stats</h1>

    <div class="mx-auto max-w-2xl text-color grid gap-5 grid-cols-1 md:grid-cols-2 rounded-lg">
      {#each statsValues as { name, value }, idx (idx)}
        <div class="flex gap-2 border rounded-lg p-4 items-center justify-between">
          <span class="text-sm">{name} </span>
          <span class="text-xs text-gray-600 dark:text-gray-300">{value}</span>
        </div>
      {/each}
    </div>
  </div>
</div>
