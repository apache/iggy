<script lang="ts">
  import Button from '$lib/components/Button.svelte';
  import Input from '$lib/components/Input.svelte';
  import SettingsLayout from '$lib/components/Layouts/SettingsLayout.svelte';
  import { invalidateIntervalDuration } from '$lib/components/PeriodicInvalidator.svelte';
  import { durationFormatter } from '$lib/utils/formatters/durationFormatter';

  let intervalValue = $state($invalidateIntervalDuration);

  let saveDisabled =
    $derived(intervalValue < 500 || intervalValue > 3600000 || intervalValue === $invalidateIntervalDuration);
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
        class="p-5 py-4 bg-shadeL200 dark:bg-shadeD900 flex items-center gap-5 justify-between"
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
