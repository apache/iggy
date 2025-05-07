import { onMount } from 'svelte';

export function addWindowEventListener<T extends keyof WindowEventMap>(
  event: T,
  eventHandler: (e: WindowEventMap[T]) => void
) {
  onMount(() => {
    window.addEventListener(event, eventHandler);
    return () => window.removeEventListener(event, eventHandler);
  });
}
