<script lang="ts" module>
  export type Theme = 'dark' | 'light' | 'system';

  export const theme = persistedStore<Theme>('theme', 'system');
</script>

<script lang="ts">
  import { run } from 'svelte/legacy';

  import { browser } from '$app/environment';
  import { persistedStore } from '$lib/utils/persistedStore';

  const setAppTheme = () => {
    if (!browser) return;

    if ($theme === 'dark') setDarkMode();
    if ($theme === 'light') setLightMode();
    if ($theme === 'system') setSystemMode();
  };

  export const setDarkMode = () => {
    document.body.classList.add('transitions-disabled');
    document.documentElement.classList.add('dark');
    void window.getComputedStyle(document.body).opacity;
    document.body.classList.remove('transitions-disabled');
  };
  export const setLightMode = () => {
    document.body.classList.add('transitions-disabled');
    document.documentElement.classList.remove('dark');
    void window.getComputedStyle(document.body).opacity;
    document.body.classList.remove('transitions-disabled');
  };

  export const setSystemMode = () => {
    if (window.matchMedia('(prefers-color-scheme: dark)').matches) {
      setDarkMode();
    } else {
      setLightMode();
    }
  };

  run(() => {
    setAppTheme();
  });
</script>
