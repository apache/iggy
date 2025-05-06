<script lang="ts">
  import Button from '$lib/components/Button.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import Input from '$lib/components/Input.svelte';
  import { openModal } from '$lib/components/Modals/AppModals.svelte';
  import { fade } from 'svelte/transition';
  import { searchQuery, selectedUsersId, usersCount } from './UsersTab.svelte';
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
                    <span >
              <Icon name="search" />
            </span>
                  {/snippet}
        </Input>
      </div>
      <!-- 
      <Button variant="contained" on:click={() => openModal('AddUserModal', {streams: })}>
        <Icon name="plus" />
        Add user
      </Button> -->
    {/if}
  </div>
</div>
