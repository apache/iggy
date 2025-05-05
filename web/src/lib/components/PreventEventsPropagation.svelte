<script lang="ts">
  interface Props {
    children?: import('svelte').Snippet;
  }

  let { children }: Props = $props();
  const stop = (e: Event) => {
    e.stopPropagation();
  };
</script>

<div onclick={stop} aria-hidden="true">
  {@render children?.()}
</div>
