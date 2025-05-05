<script lang="ts">
  interface Props {
    class?: string | undefined;
    children?: import('svelte').Snippet;
  }

  let { class: className = undefined, children }: Props = $props();
  
</script>

<div
  class={className}
  onclick={(e) => {
    e.stopPropagation();
  }}
>
  {@render children?.()}
</div>
