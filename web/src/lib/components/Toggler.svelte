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
    checked ? ' bg-green500' : 'bg-shadeL400 dark:bg-shadeD200'
  )}
>
  <input type="checkbox" class="invisible" bind:checked />

  <div
    class={twMerge(
      'absolute left-[1px] top-1/2 -translate-y-1/2 w-[26px] h-[26px] bg-white  rounded-full border border-shadeL700 dark:border-shadeD100 transition-transform ease-in-out duration-200',
      checked && 'translate-x-[24px]'
    )}
></div>
</label>
