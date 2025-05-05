<script>
  import { goto, invalidate, invalidateAll } from '$app/navigation';
  import { page } from '$app/state';
  import { typedRoute } from '$lib/types/appRoutes';
</script>

{#if page.error && page.error.message === 'Not Found'}
  Not found
{:else}
  Internal Error
{/if}
