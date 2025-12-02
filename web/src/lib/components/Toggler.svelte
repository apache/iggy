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
  import { twMerge } from 'tailwind-merge';

  interface Props {
    checked?: boolean;
  }

  let { checked = $bindable(false) }: Props = $props();
</script>

<label
  class={twMerge(
    'h-[28px] w-[52px] block rounded-full  relative transition-colors ease-in-out duration-200 hover:cursor-pointer',
    checked ? ' bg-green500' : 'bg-shade-l400 dark:bg-shade-d200'
  )}
>
  <input type="checkbox" class="invisible" bind:checked />

  <div
    class={twMerge(
      'absolute left-px top-1/2 -translate-y-1/2 w-[26px] h-[26px] bg-white  rounded-full border border-shade-l700 dark:border-shade-d100 transition-transform ease-in-out duration-200',
      checked && 'translate-x-[24px]'
    )}
  ></div>
</label>
