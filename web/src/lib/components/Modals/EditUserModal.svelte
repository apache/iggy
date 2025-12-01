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
