<script lang="ts">
  interface Props {
    data: any;
  }

  let { data }: Props = $props();

  let statsValues = $derived(Object.values(data.stats).map((val) => val as { name: string; value: string }));
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
