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
  import parseDuration from 'parse-duration';
  import Checkbox from './Checkbox.svelte';
  import Input from './Input.svelte';
  import SegmentedControl from './SegmentedControl.svelte';
  import { durationFormatter } from '$lib/utils/formatters/durationFormatter';

  interface Props {
    label?: string;
    value: number;
  }

  let { label = 'Message expiry', value = $bindable(0) }: Props = $props();

  const US_PER_DAY = 86_400 * 1_000_000;
  const US_PER_HOUR = 3_600 * 1_000_000;
  const US_PER_MIN = 60 * 1_000_000;
  const US_PER_SEC = 1_000_000;
  const MAX_US = 10 * 365 * US_PER_DAY;

  function splitToSeconds(v: number) {
    let rem = Math.floor(v / US_PER_SEC) * US_PER_SEC;
    const d = Math.floor(rem / US_PER_DAY);
    rem %= US_PER_DAY;
    const h = Math.floor(rem / US_PER_HOUR);
    rem %= US_PER_HOUR;
    const m = Math.floor(rem / US_PER_MIN);
    rem %= US_PER_MIN;
    const s = Math.floor(rem / US_PER_SEC);
    return { d, h, m, s };
  }

  const init = splitToSeconds(value);

  function buildText({ d, h, m, s }: ReturnType<typeof splitToSeconds>): string {
    const parts: string[] = [];
    if (d) parts.push(`${d}d`);
    if (h) parts.push(`${h}h`);
    if (m) parts.push(`${m}min`);
    if (s) parts.push(`${s}s`);
    return parts.join(' ');
  }

  let never = $state(value === 0);
  let mode = $state<'fields' | 'text'>('text');
  let textInput = $state(value > 0 ? buildText(init) : '');
  let error = $state('');

  let days = $state(init.d);
  let hours = $state(init.h);
  let minutes = $state(init.m);
  let seconds = $state(init.s);

  let lastSetValue = value;

  function setValueInternal(v: number) {
    value = v;
    lastSetValue = v;
  }

  $effect(() => {
    const v = value;
    if (v === lastSetValue) return;
    lastSetValue = v;
    const parts = splitToSeconds(v);
    never = v === 0;
    days = parts.d;
    hours = parts.h;
    minutes = parts.m;
    seconds = parts.s;
    textInput = v > 0 ? buildText(parts) : '';
    error = '';
  });

  function updateFromFields() {
    error = '';
    const us =
      days * US_PER_DAY + hours * US_PER_HOUR + minutes * US_PER_MIN + seconds * US_PER_SEC;
    if (us > MAX_US) {
      error = 'Maximum value is 10 years';
      return;
    }
    setValueInternal(us);
  }

  function parseText(showError = false) {
    error = '';
    if (!textInput.trim()) {
      setValueInternal(0);
      return;
    }
    if (!/[a-zA-Zµ]/.test(textInput)) {
      if (showError) error = 'Unit required. Try e.g. "1h 30m" or "45s"';
      return;
    }
    const durationInUs = parseDuration(textInput, 'us');
    if (durationInUs == null || durationInUs <= 0) {
      if (showError) error = 'Invalid format. Try e.g. "1h 30m" or "45s"';
      return;
    }
    if (durationInUs < US_PER_SEC) {
      if (showError) error = 'Minimum value is 1 second';
      return;
    }
    if (durationInUs > MAX_US) {
      if (showError) error = 'Maximum value is 10 years';
      return;
    }
    setValueInternal(durationInUs);
    const s = splitToSeconds(value);
    days = s.d;
    hours = s.h;
    minutes = s.m;
    seconds = s.s;
  }

  function onModeChange(newMode: 'fields' | 'text') {
    if (newMode === 'text') {
      textInput = value > 0 ? buildText(splitToSeconds(value)) : '';
    } else {
      parseText();
    }
    error = '';
    mode = newMode;
  }

  const preview = $derived(never ? 'never' : value > 0 ? durationFormatter(value) : '');
</script>

<div class="flex flex-col gap-2">
  <span class="text-sm text-color ml-1">{label}</span>

  <div class="flex items-center gap-2">
    <Checkbox
      bind:checked={never}
      value="never"
      onclick={() => {
        if (never) {
          mode === 'text' ? parseText() : updateFromFields();
        } else {
          setValueInternal(0);
        }
      }}
    />
    <span class="text-sm text-color select-none">Never</span>
  </div>

  {#if !never}
    <div class="flex flex-col gap-2">
      <SegmentedControl
        options={[
          { value: 'text', label: 'Aa Text' },
          { value: 'fields', label: '# Fields' }
        ]}
        value={mode}
        onchange={onModeChange}
      />
      {#if mode === 'text'}
        <Input
          name="duration-text"
          type="text"
          placeholder="e.g. &quot;1d 2h 30m&quot; or &quot;45s&quot;  (min: 1s)"
          bind:value={textInput}
          oninput={() => parseText(false)}
          onblur={() => parseText(true)}
        />
      {:else}
        <div class="flex items-center gap-2 flex-wrap">
          <div class="w-[80px]">
            <Input
              name="duration-days"
              type="number"
              label="d"
              min={0}
              max={3650}
              bind:value={days}
              oninput={updateFromFields}
            />
          </div>
          <div class="w-[80px]">
            <Input
              name="duration-hours"
              type="number"
              label="h"
              min={0}
              max={23}
              bind:value={hours}
              oninput={updateFromFields}
            />
          </div>
          <div class="w-[80px]">
            <Input
              name="duration-minutes"
              type="number"
              label="min"
              min={0}
              max={59}
              bind:value={minutes}
              oninput={updateFromFields}
            />
          </div>
          <div class="w-[80px]">
            <Input
              name="duration-seconds"
              type="number"
              label="s"
              min={0}
              max={59}
              bind:value={seconds}
              oninput={updateFromFields}
            />
          </div>
        </div>
      {/if}
      {#if error}
        <span class="text-xs text-red-500">{error}</span>
      {/if}
    </div>
  {/if}

  {#if preview}
    <span class="text-xs text-shade-d200 dark:text-shade-l700 -mt-1">{preview}</span>
  {/if}
</div>
