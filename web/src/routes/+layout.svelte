<script lang="ts">
  import LoadingIndicator from '$lib/components/LoadingIndicator.svelte';
  import ThemeController from '$lib/components/ThemeController.svelte';
  import '../styles/app.css';
  interface Props {
    children?: import('svelte').Snippet;
  }

  let { children }: Props = $props();
</script>

<ThemeController />
<LoadingIndicator />

{@render children?.()}
