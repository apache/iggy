import { writable, type Updater } from 'svelte/store';

export const persistedStore = <T>(key: string, defaultValue: T) => {
  const browser = typeof window !== 'undefined' && typeof document !== 'undefined';
  if (!browser) return writable(defaultValue);

  function updateStorage(key: string, value: T) {
    if (browser) localStorage?.setItem(key, JSON.stringify(value));
  }

  const store = writable(defaultValue, (set) => {
    const json = localStorage?.getItem(key);
    if (json) set(JSON.parse(json));

    const handleStorage = (event: StorageEvent) => {
      if (event.key === key) set(event.newValue ? JSON.parse(event.newValue) : defaultValue);
    };

    window.addEventListener('storage', handleStorage);
    return () => window.removeEventListener('storage', handleStorage);
  });

  const { set, subscribe } = store;

  return {
    set(value: T) {
      updateStorage(key, value);
      set(value);
    },
    subscribe,
    update(callback: Updater<T>) {
      return store.update((last) => {
        const value = callback(last);
        updateStorage(key, value);
        return value;
      });
    }
  };
};
