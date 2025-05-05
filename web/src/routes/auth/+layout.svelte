<script>
  import Logo from '$lib/components/Logo/Logo.svelte';

  /** @type {{children?: import('svelte').Snippet}} */
  let { children } = $props();
</script>

<div class="min-h-screen bg-shadeL100 dark:bg-shadeD700 flex flex-col items-center pt-[10vh]">
  <Logo class="h-[100px] mb-10"></Logo>
  {@render children?.()}
</div>
