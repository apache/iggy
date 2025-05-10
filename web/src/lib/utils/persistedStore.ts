/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

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
