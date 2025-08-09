<script>
  import { page } from '$app/state';
</script>

{#if page.error && page.error.message === 'Not Found'}
  Not found
{:else}
  Internal Error
{/if}
