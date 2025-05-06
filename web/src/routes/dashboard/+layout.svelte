<script>
  import AppModals from '$lib/components/Modals/AppModals.svelte';
  import Navbar from '$lib/components/Navbar.svelte';
  import AppToasts from '$lib/components/AppToasts.svelte';
  import Header from '$lib/components/Header.svelte';

  /** @type {{data: any, children?: import('svelte').Snippet}} */
  let { data, children } = $props();
</script>

<div id="popupsRoot"></div>
<AppModals />
<AppToasts />

<Navbar />
<Header user={data.user} />

<main
  class="fixed left-[90px] top-[55px] bottom-0 right-0 bg-shadeL100 dark:bg-shadeD700 flex flex-col"
>
  {@render children?.()}
</main>
