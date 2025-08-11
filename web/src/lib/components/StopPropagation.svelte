<script lang="ts">
  import { Keys } from '$lib/utils/constants/keys';
  
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
  onkeydown={(e) => {
    if (e.key === Keys.ENTER || e.key === Keys.SPACE) {
      e.preventDefault();
      e.stopPropagation();
    }
  }}
  role="button"
  tabindex="0"
  aria-label="Stop propagation"
>
  {@render children?.()}
</div>
