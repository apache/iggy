<!--
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

   http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.
-->

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
