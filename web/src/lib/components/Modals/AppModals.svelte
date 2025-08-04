<script lang="ts" module>
  import type { ComponentProps } from 'svelte';
  import AddPartitionsModal from './AddPartitionsModal.svelte';
  import AddStreamModal from './AddStreamModal.svelte';
  import AddTopicModal from './AddTopicModal.svelte';
  import AddUserModal from './AddUserModal.svelte';
  import DeletePartitionsModal from './DeletePartitionsModal.svelte';
  import DeleteUserModal from './DeleteUserModal.svelte';
  import EditUserModal from './EditUserModal.svelte';
  import EditUserPermissionsModal from './EditUserPermissionsModal.svelte';
  import InspectMessage from './InspectMessage.svelte';
  import StreamSettingsModal from './StreamSettingsModal.svelte';
  import TopicSettingsModal from './TopicSettingsModal.svelte';
  import { fade } from 'svelte/transition';
  import { noTypeCheck } from '$lib/utils/noTypeCheck';
  import { writable } from 'svelte/store';
  import { Keys } from '$lib/utils/constants/keys';

  const modals = {
    AddPartitionsModal,
    AddStreamModal,
    AddTopicModal,
    AddUserModal,
    DeletePartitionsModal,
    DeleteUserModal,
    EditUserModal,
    EditUserPermissionsModal,
    InspectMessage,
    StreamSettingsModal,
    TopicSettingsModal,
  };

  type DistributiveOmit<T, K extends string> = T extends T ? Omit<T, K> : never;
  type AllModals = keyof typeof modals;
  type ModalProps<T extends AllModals> = ComponentProps<(typeof modals)[T]>;
  type ExtraProps<T extends AllModals> = DistributiveOmit<ModalProps<T>, 'closeModal'>;

  const openedModal = writable<
    | {
        modal: AllModals;
        props: ModalProps<AllModals>;
      }
    | undefined
  >();

  export function openModal<T extends AllModals>(
    modal: T,
    ...args: ExtraProps<T> extends Record<string, never> ? [] : [ExtraProps<T>]
  ) {
    const props = args[0] || {};

    openedModal.set({
      modal,
      props: {
        closeModal: (cb) => {
          return new Promise<void>((res, _) => {
            openedModal.set(undefined);
            setTimeout(async () => {
              if (cb) cb();
              res();
            }, 250);
          });
        },
        ...props
      }
    });
  }
</script>

<svelte:window
  onkeydown={(e) => {
    if (e.key === Keys.ESCAPE && $openedModal) {
      $openedModal?.props.closeModal();
    }
  }}
/>

{#if $openedModal}
  <div 
    transition:fade={{ duration: 100 }} 
    class="fixed inset-0 bg-black/40 z-[500]" 
    onclick={() => $openedModal.props.closeModal()}
    onkeydown={(e) => {
       if (e.key === Keys.ENTER || e.key === Keys.SPACE) {
         e.preventDefault();
         $openedModal?.props.closeModal();
       }
     }}
    role="button" 
    tabindex="0"
    aria-label="Close modal"
  ></div>
  {@const SvelteComponent_1 = noTypeCheck(modals[$openedModal.modal])}
  <SvelteComponent_1
    {...$openedModal.props}
    closeModal={$openedModal.props.closeModal}
  />
{/if}
