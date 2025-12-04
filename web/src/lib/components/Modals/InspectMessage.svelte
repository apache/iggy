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
  import ModalBase from './ModalBase.svelte';
  import type { CloseModalFn } from '$lib/types/utilTypes';
  import { type Message, type HeaderField } from '$lib/domain/Message';
  import MessageDecoder from '$lib/components/MessageDecoder/MessageDecoder.svelte';
  import { decodeBase64 } from '$lib/utils/base64Utils';
  import { formatMessageId } from '$lib/utils/formatters/uuidFormatter';

  interface Props {
    closeModal: CloseModalFn;
    message: Message;
  }

  let { closeModal, message }: Props = $props();

  const formattedId = $derived(message?.id ? formatMessageId(message.id) : 'N/A');

  const formatHeaders = (headers: Record<string, HeaderField> | null | undefined) => {
    if (!headers || Object.keys(headers).length === 0) {
      return 'No headers';
    }
    try {
      return JSON.stringify(headers, null, 2);
    } catch {
      return 'Invalid headers format';
    }
  };

  // TODO: whether all header values should be decoded?
  let codec = $derived(
    message?.user_headers?.['codec']?.value
      ? decodeBase64(message.user_headers['codec'].value)
      : undefined
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
        <div class="text-sm text-color font-medium">{message?.checksum ?? 'N/A'}</div>
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
        <div class="text-sm text-color font-medium font-mono whitespace-pre-wrap">
          {formatHeaders(message?.user_headers)}
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
