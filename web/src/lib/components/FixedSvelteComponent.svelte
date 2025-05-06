<script lang="ts" generics="T extends ComponentType">
  import { run } from 'svelte/legacy';

  import type { ComponentProps, ComponentType, SvelteComponent } from 'svelte';

  interface Props {
    component: T;
    props: ComponentProps<InstanceType<T>>;
  }

  let { component, props }: Props = $props();

  let target: HTMLElement | null = $state(null);
  let prevComponent: SvelteComponent | null = $state(null);

  run(() => {
    if (component && props && target) {
      if (prevComponent) {
        prevComponent.$destroy();
      }

      prevComponent = new component({ target, props, hydrate: true });
    }
  });
</script>

<div bind:this={target}></div>
