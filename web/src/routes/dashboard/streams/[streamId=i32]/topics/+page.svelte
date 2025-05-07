<script>
  import { goto } from '$app/navigation';
  import { typedRoute } from '$lib/types/appRoutes';
  import { onMount } from 'svelte';

  onMount(() => {
    goto(typedRoute('/dashboard/streams'));
  });
</script>
