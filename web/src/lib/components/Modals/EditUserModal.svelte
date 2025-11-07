<script lang="ts">
  import type { CloseModalFn } from '$lib/types/utilTypes';

  import ModalBase from './ModalBase.svelte';

  interface Props {
    closeModal: CloseModalFn;
  }

  let { closeModal }: Props = $props();
</script>

<ModalBase {closeModal} title="Edit user">
  <div class="h-[300px] flex flex-col">
    <!-- <div class="flex justify-end gap-3 mt-auto">
              <Button variant="text" class="w-2/5" onclick={closeModal}>Cancel</Button>
              <Button type="submit" variant="contained" class="w-2/5">Save</Button>
            </div> -->
  </div>
</ModalBase>
