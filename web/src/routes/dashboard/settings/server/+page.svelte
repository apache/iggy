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
  import Button from '$lib/components/Button.svelte';
  import SettingsLayout from '$lib/components/Layouts/SettingsLayout.svelte';

  import RangeInput from '$lib/components/RangeInput.svelte';
  import Toggler from '$lib/components/Toggler.svelte';

  interface Props {
    data: any;
  }

  let { data }: Props = $props();

  let cacheEnabled = $state(false);

  let cacheValue = $state(0);
</script>

<SettingsLayout>
  <div class="p-5">
    <section
      class="border rounded-md overflow-hidden mx-auto max-w-[900px] text-color text-shade-l1000"
    >
      <div class="flex flex-col p-7 gap-3">
        <h3 class="text-2xl font-semibold">Cache</h3>
        <label class="flex gap-4 mt-2 items-center hover:cursor-pointer w-fit">
          <Toggler bind:checked={cacheEnabled} />
          <span>{cacheEnabled ? 'Enabled' : 'Disabled'} </span>
        </label>

        {#if cacheEnabled}
          <div>
            <span class="block">Adjust amount of server cache.</span>
            <div class="flex items-center justify-center gap-4 w-fit">
              <div class="w-[270px]">
                <RangeInput
                  className="w-[270px] h-[9px]"
                  size="big"
                  initValue={50}
                  bind:value={cacheValue}
                />
              </div>
              <span> {cacheValue}</span>
            </div>
          </div>
        {/if}
      </div>
      <footer class="p-5 py-4 bg-shade-l200 dark:bg-shade-d900 flex items-center justify-between">
        <span class=" text-sm">
          max cache size: 80% of {data.serverStats.availableMemory.value}
        </span>
        <Button disabled variant="contained">Save</Button>
      </footer>
    </section>
  </div>
</SettingsLayout>
