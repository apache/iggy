<script lang="ts">
  import Button from '../Button.svelte';
  import Select from '../Select.svelte';
  import { decoderRegistry } from './decoders/utils/decoderRegistry';
  import { decodeBase64 } from '$lib/utils/base64Utils';

  interface Props {
    payload?: string | undefined;
    codec?: string | undefined | null;
  }

  let { payload = undefined, codec = undefined }: Props = $props();

  type ErrorState = { title: string; message: string };
  type DecodedState =
    | { status: 'idle'; forceChangeDecoder: boolean }
    | { status: 'error'; error: ErrorState }
    | { status: 'success'; content: string };

  let decodeState = $state<DecodedState>({ status: 'idle', forceChangeDecoder: false });
  let showRaw = $state(false);
  let selectedDecoder = $state(codec || 'string');

  function getError(type: 'no_payload' | 'unsupported_codec' | 'invalid_format'): ErrorState {
    switch (type) {
      case 'no_payload':
        return {
          title: 'No payload',
          message: 'This message has no payload to decode'
        };
      case 'unsupported_codec':
        return {
          title: 'Unsupported codec',
          message: `The specified codec ${codec} is not supported. Select another decoder from the dropdown below.`
        };
      case 'invalid_format':
        return {
          title: 'Invalid format',
          message: `The payload is not valid ${selectedDecoder.toUpperCase()}`
        };
    }
  }

  function decode() {
    if (!payload) {
      decodeState = { status: 'error', error: getError('no_payload') };
      return;
    }

    const decodedPayload = decodeBase64(payload);
    if (!decodedPayload) {
      decodeState = { status: 'error', error: getError('invalid_format') };
      return;
    }

    const decoder = decoderRegistry.get(selectedDecoder);
    if (!decoder) {
      decodeState = { status: 'error', error: getError('unsupported_codec') };
      return;
    }

    const content = decoder.decode(decodedPayload);
    if (!content) {
      decodeState = { status: 'error', error: getError('invalid_format') };
      return;
    }

    decodeState = { status: 'success', content };
  }
</script>

{#if decodeState.status === 'idle'}
  {#if !codec || !decoderRegistry.get(codec) || decodeState.forceChangeDecoder}
    <div class="mb-4">
      <Select
        label="Select decoder"
        name="decoder"
        options={decoderRegistry.getNames()}
        bind:value={selectedDecoder}
      />
    </div>
  {/if}
  <Button variant="contained" onclick={decode}>Decode</Button>
{:else}
  {#if decodeState.status === 'error'}
    <div class="bg-red-100 dark:bg-red-900 p-4 rounded-md w-full">
      <p class="font-bold">{decodeState.error.title}</p>
      <p>{decodeState.error.message}</p>
    </div>
  {/if}

  {#if decodeState.status === 'success'}
    <div class="flex items-center gap-3">
      <p class="font-bold">Codec: {selectedDecoder}</p>
      <Button variant="text" onclick={() => (decodeState = { status: 'idle', forceChangeDecoder: true })}>Change decoder</Button>
    </div>
    <div class="w-full mt-2">
      <pre class="bg-gray-100 dark:bg-gray-800 p-1 rounded-md overflow-auto"><code
          >{decodeState.content}</code
        ></pre>
    </div>
  {/if}

  <div class="mt-4 mb-2">
    <Button variant="text" onclick={() => (showRaw = !showRaw)}>
      {showRaw ? 'Hide' : 'Show'} raw payload (base64)
    </Button>
  </div>

  {#if showRaw && payload}
    <div class="w-full">
      <pre
        class="bg-gray-100 dark:bg-gray-800 p-4 rounded-md overflow-auto whitespace-pre-wrap break-all"><code
          >{payload}</code
        ></pre>
    </div>
  {/if}
{/if}
