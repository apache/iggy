<script lang="ts" module>
  import { writable } from 'svelte/store';
  export const searchQuery = writable('');
  export const usersCount = writable<undefined | number>();
  export const selectedUsersId = writable<string[]>([]);
</script>

<script lang="ts">
  import type { User } from '$lib/domain/User';
  import SlimSortableList from '$lib/components/SlimSortableList.svelte';
  import Checkbox from '$lib/components/Checkbox.svelte';
  import { twMerge } from 'tailwind-merge';
  import Button from '$lib/components/Button.svelte';
  import Icon, { type iconType } from '$lib/components/Icon.svelte';
  import DropdownMenu from '$lib/components/DropdownMenu/DropdownMenu.svelte';
  import StopPropagation from '$lib/components/StopPropagation.svelte';
  import { openModal } from '$lib/components/Modals/AppModals.svelte';

  interface Props {
    users: Array<User>;
  }

  let { users }: Props = $props();

  $usersCount = users.length;
  let filteredUsers = $derived(users.filter((user) =>
    user.username.toLowerCase().includes($searchQuery.toLowerCase())
  ));

  const userActions = [
    {
      label: 'Edit',
      icon: 'editPen',
      action: () => {
        openModal('EditUserModal');
      }
    },
    {
      label: 'Permissions',
      icon: 'shieldLock',
      action: () => {
        openModal('EditUserPermissionsModal');
      }
    },
    {
      label: 'Delete',
      icon: 'trash',
      action: () => {
        openModal('DeleteUserModal');
      }
    }
  ] satisfies { label: string; icon: iconType; action: VoidFunction }[];

  const toggleAllChecked = (e: Event) => {
    const { checked } = e.target as HTMLInputElement;

    $selectedUsersId = checked
      ? users.filter((user) => user.id !== 1).map((user) => `${user.id}`)
      : [];
  };

  let allChecked = $derived(users
    .filter((user) => user.id !== 1)
    .every((user) => $selectedUsersId.includes(user.id.toString())));
</script>

<SlimSortableList
  data={filteredUsers}
  emptyDataMessage=""
  gridColsClass="grid-cols-[100px_150px_3fr_2fr_2fr_120px]"
  columns={[
    {
      slot: true
    },
    {
      key: 'id',
      label: 'Id',
      sortable: true
    },
    {
      key: 'username',
      label: 'Username',
      sortable: true
    },
    {
      key: 'createdAt',
      label: 'Created',
      sortable: true
    },
    {
      key: 'status',
      label: 'Status',
      sortable: true
    },
    { label: 'Actions', sortable: false }
  ]}


>
  {#snippet header()}
    <div class="flex items-center justify-center" >
      <Checkbox value="all" checked={allChecked} onclick={toggleAllChecked} />
    </div>
  {/snippet}

  {#snippet children({ item: row, baseClass })}
    <label
      for="{row.id}-{row.username}"
      class={twMerge(
        baseClass,
        row.id === 1 && 'bg-shadeL800 dark:bg-shadeD1000 pointer-events-none',
        $selectedUsersId.includes(row.id.toString()) &&
          'ring-2 ring-inset ring-green500 !bg-green-300/30  '
      )}
    >
      <div class="flex items-center justify-center">
        {#if row.id !== 1}
          <Checkbox
            value={row.id.toString()}
            bind:bindGroup={$selectedUsersId}
            checked={false}
            id="{row.id}-{row.username}"
          />
        {/if}
      </div>
      <div class="px-5 font-semibold ">
        {row.id}
      </div>

      <div class="px-5">
        <span class="whitespace-nowrap">
          {row.username}
        </span>
      </div>

      <div class="px-5  whitespace-nowrap">
        {row.createdAt}
      </div>

      <div class="px-5">
        <span
          class={twMerge(
            'rounded-full block w-[70px] text-center p-1 text-white text-sm capitalize',
            row.status === 'active' ? 'bg-green500' : 'bg-shadeD100 dark:bg-shadeD400'
          )}
        >
          {row.status}
        </span>
      </div>
      <div class="px-5">
        <StopPropagation>
          <DropdownMenu placement="left-start" >
            {#snippet trigger()}
                    <Button variant="rounded" class="" >
                <Icon name="verticalDots" />
              </Button>
                  {/snippet}
            {#snippet children({ close })}
                    <div>
                {#each userActions as { action, icon, label }}
                  <button
                    onclick={() => {
                      action();
                      close();
                    }}
                    class={twMerge(
                      'grid grid-cols-[20px,1fr] gap-x-1 rounded-md items-center w-full px-2 py-2 text-sm text-color cursor-default hoverable-strong'
                    )}
                  >
                    <span>
                      <Icon name={icon} class="w-[17px] h-[17px]" />
                    </span>
                    <span class={twMerge('text-left')}>
                      {label}
                    </span>
                  </button>
                {/each}
              </div>
                              {/snippet}
                </DropdownMenu>
        </StopPropagation>
      </div>
    </label>
  {/snippet}
</SlimSortableList>
