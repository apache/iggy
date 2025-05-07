import { afterNavigate } from '$app/navigation';
import { base } from '$app/paths';
import { writable } from 'svelte/store';

export const useGoPrev = () => {
  const { subscribe, set } = writable(base.toString());

  afterNavigate(({ from }) => {
    set(from?.url?.pathname || base);
  });

  return { prevPage: { subscribe } };
};
