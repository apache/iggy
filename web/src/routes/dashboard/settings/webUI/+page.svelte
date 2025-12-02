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
  import Input from '$lib/components/Input.svelte';
  import SettingsLayout from '$lib/components/Layouts/SettingsLayout.svelte';
  import { invalidateIntervalDuration } from '$lib/components/PeriodicInvalidator.svelte';
  import { durationFormatter } from '$lib/utils/formatters/durationFormatter';

  let intervalValue = $state($invalidateIntervalDuration);

  let saveDisabled = $derived(
    intervalValue < 500 || intervalValue > 3600000 || intervalValue === $invalidateIntervalDuration
  );
</script>

<SettingsLayout>
  <div class="p-5">
    <section class="border rounded-md overflow-hidden mx-auto max-w-[900px] text-color-gray">
      <div class="flex flex-col p-7 gap-3">
        <h3 class="text-2xl font-semibold">Refetch interval</h3>
        <div class="flex gap-4 mt-2 items-center">
          <div class="w-[130px]">
            <Input type="number" name="interval" bind:value={intervalValue} />
          </div>
          <span>ms</span>
          {#if intervalValue >= 1000}
            <span class="text-sm">
              ({durationFormatter(Math.round(intervalValue / 1000))})
            </span>
          {/if}
        </div>
      </div>
      <footer
        class="p-5 py-4 bg-shade-l200 dark:bg-shade-d900 flex items-center gap-5 justify-between"
      >
        <span class=" text-sm">
          The entire server state will be refreshed at provided intervals. Max: 1 hour, min: 500 ms.
        </span>
        <Button
          disabled={saveDisabled}
          onclick={() => {
            $invalidateIntervalDuration = intervalValue;
          }}
          variant="contained">Save</Button
        >
      </footer>
    </section>
  </div>
</SettingsLayout>
