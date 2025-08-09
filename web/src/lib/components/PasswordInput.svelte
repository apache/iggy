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
    [key: string]: any
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
    <span >
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
          class="w-[20px] h-[20px] dark:stroke-white stroke-shadeD200"
        />
      </Button>
    </span>
  {/snippet}
</Input>
