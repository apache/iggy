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
  class="fixed left-[90px] top-[55px] bottom-0 right-0 bg-shade-l100 dark:bg-shade-d700 flex flex-col"
>
  {@render children?.()}
</main>
