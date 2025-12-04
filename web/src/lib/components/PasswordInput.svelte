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
  import Input from '$lib/components/Input.svelte';
  import Icon from './Icon.svelte';
  import Button from './Button.svelte';

  interface Props {
    label: string;
    id?: string;
    name: string;
    errorMessage?: string;
    value: string | number;
    [key: string]: any;
  }

  let {
    label,
    id = undefined,
    name,
    errorMessage = undefined,
    value = $bindable(),
    ...rest
  }: Props = $props();

  let isVisible = $state(false);
</script>

<Input
  {label}
  {name}
  {id}
  type={isVisible ? 'text' : 'password'}
  bind:value
  {errorMessage}
  {...rest}
>
  {#snippet suffix()}
    <span>
      <Button
        variant="rounded"
        class="w-[33px] h-[33px] p-0 flex items-center justify-center"
        onclick={(e) => {
          isVisible = !isVisible;
          e.preventDefault();
          e.stopPropagation();
        }}
      >
        <Icon
          name={isVisible ? 'eye' : 'eyeOff'}
          class="w-[20px] h-[20px] dark:stroke-white stroke-shade-d200"
        />
      </Button>
    </span>
  {/snippet}
</Input>
