<script lang="ts">
  let className = '';
  export { className as class };
  const lightLogo = "/iggy-apache-lightbg.svg";
  const darkLogo = "/iggy-apache-darkbg.svg";
</script>

<img src={lightLogo} class="{className} block dark:hidden" alt="Apache Iggy" />
<img src={darkLogo} class="{className} hidden dark:block" alt="Apache Iggy" />
