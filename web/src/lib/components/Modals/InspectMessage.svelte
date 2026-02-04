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
  import { SvelteSet } from 'svelte/reactivity';
  import ModalBase from './ModalBase.svelte';
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import { type Message, type HeaderEntry, type HeaderField } from '$lib/domain/Message';
  import MessageDecoder from '$lib/components/MessageDecoder/MessageDecoder.svelte';

  import { formatMessageId } from '$lib/utils/formatters/uuidFormatter';

  interface Props {
    closeModal: CloseModalFn;
    message: Message;
  }

  let { closeModal, message }: Props = $props();

  const formattedId = $derived(message?.id ? formatMessageId(message.id) : 'N/A');

  const decodeHeaderValue = (kind: string, base64Value: string): string => {
    try {
      const binaryString = atob(base64Value);
      const bytes = new Uint8Array(binaryString.length);
      for (let i = 0; i < binaryString.length; i++) {
        bytes[i] = binaryString.charCodeAt(i);
      }
      const view = new DataView(bytes.buffer);

      switch (kind.toLowerCase()) {
        case 'string':
          return new TextDecoder().decode(bytes);
        case 'bool':
          return bytes[0] !== 0 ? 'true' : 'false';
        case 'int8':
          return view.getInt8(0).toString();
        case 'int16':
          return view.getInt16(0, true).toString();
        case 'int32':
          return view.getInt32(0, true).toString();
        case 'int64':
          return view.getBigInt64(0, true).toString();
        case 'int128': {
          const lo = view.getBigUint64(0, true);
          const hi = view.getBigInt64(8, true);
          return (hi * BigInt(2 ** 64) + lo).toString();
        }
        case 'uint8':
          return view.getUint8(0).toString();
        case 'uint16':
          return view.getUint16(0, true).toString();
        case 'uint32':
          return view.getUint32(0, true).toString();
        case 'uint64':
          return view.getBigUint64(0, true).toString();
        case 'uint128': {
          const lo = view.getBigUint64(0, true);
          const hi = view.getBigUint64(8, true);
          return (hi * BigInt(2 ** 64) + lo).toString();
        }
        case 'float32':
          return view.getFloat32(0, true).toString();
        case 'float64':
          return view.getFloat64(0, true).toString();
        default:
          return base64Value;
      }
    } catch {
      return base64Value;
    }
  };

  let expandedHeaders = new SvelteSet<number>();

  const toggleHeaderExpand = (index: number) => {
    if (expandedHeaders.has(index)) {
      expandedHeaders.delete(index);
    } else {
      expandedHeaders.add(index);
    }
  };

  const findHeaderByStringKey = (
    headers: HeaderEntry[] | undefined,
    keyName: string
  ): HeaderField | undefined => {
    if (!headers) return undefined;
    const entry = headers.find(
      (e) => e.key.kind === 'string' && decodeHeaderValue('string', e.key.value) === keyName
    );
    return entry?.value;
  };

  let codec = $derived(
    (() => {
      const codecHeader = findHeaderByStringKey(message?.user_headers, 'codec');
      return codecHeader ? decodeHeaderValue(codecHeader.kind, codecHeader.value) : undefined;
    })()
  );
</script>

<ModalBase {closeModal} title="Message details" class="w-full max-w-[90vw] lg:max-w-[80vw]">
  <div class="grid grid-cols-1 lg:grid-cols-2 gap-6 p-4 lg:p-6 h-full w-full overflow-auto">
    <div class="flex flex-col gap-4">
      <div class="bg-shade-l200 dark:bg-shade-d400 p-3 lg:p-4 rounded-md">
        <span class="text-xs text-shade-l900 dark:text-shade-l700 block mb-1">ID</span>
        <div class="text-sm text-color font-medium font-mono break-all">
          {formattedId}
          {#if formattedId !== message?.id && message?.id}
            <span class="text-xs text-shade-l900 dark:text-shade-l700 block mt-1">
              Raw: {message.id}
            </span>
          {/if}
        </div>
      </div>

      <div class="bg-shade-l200 dark:bg-shade-d400 p-3 lg:p-4 rounded-md">
        <span class="text-xs text-shade-l900 dark:text-shade-l700 block mb-1">Offset</span>
        <div class="text-sm text-color font-medium">{message?.offset ?? 'N/A'}</div>
      </div>

      <div class="bg-shade-l200 dark:bg-shade-d400 p-3 lg:p-4 rounded-md">
        <span class="text-xs text-shade-l900 dark:text-shade-l700 block mb-1">Checksum</span>
        <div class="text-sm text-color font-medium font-mono">
          {#if message?.checksum != null}
            0x{BigInt(message.checksum).toString(16).toUpperCase()}
            <span class="text-xs text-shade-l900 dark:text-shade-l700 ml-2">
              ({message.checksum})
            </span>
          {:else}
            N/A
          {/if}
        </div>
      </div>

      <div class="bg-shade-l200 dark:bg-shade-d400 p-3 lg:p-4 rounded-md">
        <span class="text-xs text-shade-l900 dark:text-shade-l700 block mb-1">Timestamp</span>
        <div class="text-sm text-color font-medium">
          {message?.formattedTimestamp ?? 'N/A'}
          <span class="text-xs text-shade-l900 dark:text-shade-l700 ml-2">
            ({message?.timestamp ?? 'N/A'})
          </span>
        </div>
      </div>

      <div class="bg-shade-l200 dark:bg-shade-d400 p-3 lg:p-4 rounded-md">
        <span class="text-xs text-shade-l900 dark:text-shade-l700 block mb-1">Headers</span>
        <div class="text-sm text-color font-medium font-mono max-h-48 overflow-y-auto">
          {#if !message?.user_headers || message.user_headers.length === 0}
            <span class="text-shade-l900 dark:text-shade-l700">No headers</span>
          {:else}
            <div class="flex flex-col gap-1">
              {#each message.user_headers as entry, index (index)}
                {@const keyValue = decodeHeaderValue(entry.key.kind, entry.key.value)}
                {@const valueValue = decodeHeaderValue(entry.value.kind, entry.value.value)}
                {@const isExpanded = expandedHeaders.has(index)}
                <div class="flex flex-col">
                  <div class="flex items-center gap-1">
                    <button
                      type="button"
                      onclick={() => toggleHeaderExpand(index)}
                      class="text-shade-l900 dark:text-shade-l700 hover:text-color transition-colors text-xs w-4"
                      title="Show type details"
                    >
                      {isExpanded ? '▼' : '▶'}
                    </button>
                    <span class="text-color">{keyValue}</span>
                    <span class="text-shade-l900 dark:text-shade-l700 mx-1">→</span>
                    <span class="text-color">{valueValue}</span>
                  </div>
                  {#if isExpanded}
                    <div class="ml-5 text-xs text-shade-l900 dark:text-shade-l700">
                      key: {entry.key.kind}, value: {entry.value.kind}
                    </div>
                  {/if}
                </div>
              {/each}
            </div>
          {/if}
        </div>
      </div>
    </div>

    <div class="bg-shade-l200 dark:bg-shade-d400 p-3 lg:p-4 rounded-md">
      <span class="text-xs text-shade-l900 dark:text-shade-l700 block mb-1">Payload</span>
      <div class="text-sm text-color font-medium">
        <MessageDecoder payload={message?.payload} {codec} />
      </div>
    </div>
  </div>
</ModalBase>
