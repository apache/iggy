<script lang="ts">
  import Button from '$lib/components/Button.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import { createEventDispatcher } from 'svelte';

  interface Props {
    currentPage: number;
    totalPages: number;
    maxVisiblePages?: number;
  }

  let { currentPage, totalPages, maxVisiblePages = 5 }: Props = $props();


  const dispatch = createEventDispatcher<{ pageChange: number }>();

  function getVisiblePages(currentPage: number, totalPages: number, maxVisiblePages: number) {
    let startPage: number, endPage: number;

    if (totalPages <= maxVisiblePages) {
      startPage = 1;
      endPage = totalPages;
    } else {
      const maxPagesBeforeCurrentPage = Math.floor(maxVisiblePages / 2);
      const maxPagesAfterCurrentPage = Math.ceil(maxVisiblePages / 2) - 1;

      if (currentPage <= maxPagesBeforeCurrentPage) {
        startPage = 1;
        endPage = maxVisiblePages;
      } else if (currentPage + maxPagesAfterCurrentPage >= totalPages) {
        startPage = totalPages - maxVisiblePages + 1;
        endPage = totalPages;
      } else {
        startPage = currentPage - maxPagesBeforeCurrentPage;
        endPage = currentPage + maxPagesAfterCurrentPage;
      }
    }

    return Array.from({ length: endPage - startPage + 1 }, (_, i) => startPage + i);
  }

  function emitPageChange(page: number) {
    dispatch('pageChange', page);
  }
  let visiblePages = $derived(getVisiblePages(currentPage, totalPages, maxVisiblePages));
</script>

<div class="flex justify-center items-center space-x-2">
  <Button
    variant="text"
    onclick={() => emitPageChange(currentPage - 1)}
    disabled={currentPage === 1}
  >
    <Icon name="arrowLeft" />
  </Button>

  {#if visiblePages[0] > 1}
    <Button variant="text" onclick={() => emitPageChange(1)}>1</Button>
    {#if visiblePages[0] > 2}
      <span class="px-2">...</span>
    {/if}
  {/if}

  {#each visiblePages as page}
    <Button
      variant={currentPage === page ? 'contained' : 'text'}
      onclick={() => emitPageChange(page)}
    >
      {page}
    </Button>
  {/each}

  {#if visiblePages[visiblePages.length - 1] < totalPages}
    {#if visiblePages[visiblePages.length - 1] < totalPages - 1}
      <span class="px-2">...</span>
    {/if}
    <Button variant="text" onclick={() => emitPageChange(totalPages)}>{totalPages}</Button>
  {/if}

  <Button
    variant="text"
    onclick={() => emitPageChange(currentPage + 1)}
    disabled={currentPage === totalPages}
  >
    <Icon name="arrowRight" />
  </Button>
</div>
