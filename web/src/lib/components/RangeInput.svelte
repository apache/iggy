<script lang="ts">
  import { twMerge } from 'tailwind-merge';

  const sizes = {
    small: '[&::-webkit-slider-thumb]:w-[13px] [&::-webkit-slider-thumb]:h-[13px] h-[4px]',
    medium: '[&::-webkit-slider-thumb]:w-[15px] [&::-webkit-slider-thumb]:h-[15px] h-[6px]',
    big: '[&::-webkit-slider-thumb]:w-[20px] [&::-webkit-slider-thumb]:h-[20px] h-[8px]'
  };

  interface Props {
    initValue: number;
    className?: string;
    size?: keyof typeof sizes;
    value?: any;
  }

  let {
    initValue,
    className = '',
    size = 'medium',
    value = $bindable(initValue)
  }: Props = $props();
</script>

<div class=" w-fit relative flex items-center justify-center">
  <input
    type="range"
    bind:value
    class={twMerge('progress', sizes[size], className)}
    style="background:linear-gradient(to right, {value > 70
      ? '#f0f01f'
      : 'var(--green500)'}  0%, {value}%, var(--shadeL500) {value}%, var(--shadeL500) 100%)"
  />
</div>

<style>
  .progress {
    position: relative;
    z-index: 10;
    border-radius: 8px;
    outline: none;
    transition: background 450ms ease-in;
    -webkit-appearance: none;
    appearance: none;
  }

  .progress::-webkit-slider-thumb {
    border-radius: 50%;
    -webkit-appearance: none;
    cursor: ew-resize;
    background: var(--shadeL500);
    border: 1px solid var(--shadeL800);
  }

  .progress::-webkit-slider-thumb:hover {
    cursor: ew-resize;
  }
</style>
