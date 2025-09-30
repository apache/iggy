<script lang="ts">
</script>

<div class="flex items-baseline gap-1 text-gray-400">
  {#each [0, 200, 400] as delay (delay)}
    <span
      class="loader-animation w-[4px] h-[4px] bg-gray-400 rounded-full"
      style="animation-delay:{delay}ms "
    ></span>
  {/each}
</div>

<style lang="postcss">
  @reference "../../styles/app.css";
  .loader-animation {
    animation: dots-animation 700ms infinite alternate;
  }

  @keyframes dots-animation {
    from {
      opacity: 0.1;
    }

    to {
      opacity: 1;
    }
  }
</style>
