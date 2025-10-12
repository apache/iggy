<script>
  import { goto } from '$app/navigation';
  import { typedRoute } from '$lib/types/appRoutes';
  import { onMount } from 'svelte';
  import { resolve } from '$app/paths';

  onMount(() => {
    goto(resolve(typedRoute('/dashboard/streams')));
  });
</script>
