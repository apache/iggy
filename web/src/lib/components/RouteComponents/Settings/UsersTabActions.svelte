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

<script lang="ts">
  import Icon from '$lib/components/Icon.svelte';
  import Input from '$lib/components/Input.svelte';
  import { searchQuery, usersCount } from './UsersTab.svelte';
</script>

<div class="flex flex-col-reverse lg:flex-row gap-3 lg:gap-5 items-center">
  <!-- {#if $selectedUsersId.length > 0}
    <div class="mr-auto flex gap-2" transition:fade={{ duration: 50 }}>
      <Button variant="containedRed">
        <Icon name="trash" />
        Delete selected</Button
      >
      <Button variant="contained">
        <Icon name="shieldLock" />
        Change permissions</Button
      >
    </div>
  {/if} -->

  <div class="flex items-center gap-4">
    {#if $usersCount}
      <span class="text-sm text-color-gray"> {$usersCount} users </span>
      <div class="max-w-[170px]">
        <Input bind:value={$searchQuery} placeholder="Search" name="search">
          {#snippet prefix()}
            <span>
              <Icon name="search" />
            </span>
          {/snippet}
        </Input>
      </div>
      <!--
      <Button variant="contained" onclick={() => openModal('AddUserModal', {streams: })}>
        <Icon name="plus" />
        Add user
      </Button> -->
    {/if}
  </div>
</div>
