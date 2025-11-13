<script lang="ts">
  import { onNavigate } from '$app/navigation';
  import { Tween } from 'svelte/motion';
  import { cubicInOut } from 'svelte/easing';

  const progress = new Tween(0);
  const opacity = new Tween(0);

  let timer = $state<ReturnType<typeof setInterval>>();

  async function start() {
    clearInterval(timer);
    await progress.set(0, { duration: 0 });
    await opacity.set(1, { duration: 0 });

    await progress.set(16, { duration: 250, easing: cubicInOut });

    timer = setInterval(() => {
      progress.set(
        progress.current < 90
          ? progress.current + Math.floor(Math.random() * (10 - 4 + 1) + 4)
          : progress.current,
        {
          duration: 300,
          easing: cubicInOut
        }
      );
    }, 550);
  }

  async function stop() {
    await progress.set(100, { duration: 100, easing: cubicInOut });
    await opacity.set(0, { duration: 200, delay: 300 });
  }

  onNavigate(() => {
    start();
    return () => {
      stop();
    };
  });
</script>

<div class="fixed left-0 right-0 top-0 pointer-events-none z-999">
  <div
    class="bg-green500 h-[2px]"
    style="width: {progress.current}%; opacity:{opacity.current};"
  ></div>
</div>
