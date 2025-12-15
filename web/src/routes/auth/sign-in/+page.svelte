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
  import { goto } from '$app/navigation';
  import { resolve } from '$app/paths';
  import { env } from '$env/dynamic/public';
  import { authStore } from '$lib/auth/authStore.svelte';
  import Button from '$lib/components/Button.svelte';
  import Checkbox from '$lib/components/Checkbox.svelte';
  import Icon from '$lib/components/Icon.svelte';
  import Input from '$lib/components/Input.svelte';
  import PasswordInput from '$lib/components/PasswordInput.svelte';
  import { typedRoute } from '$lib/types/appRoutes';
  import { persistedStore } from '$lib/utils/persistedStore.js';
  import { onMount } from 'svelte';

  let username = $state('');
  let password = $state('');
  let errorMessage = $state('');
  let isLoading = $state(false);
  let usernameError = $state('');
  let passwordError = $state('');

  const remember = persistedStore('rememberMe', { rememberMe: true, username: '' });

  onMount(() => {
    if ($remember.rememberMe) {
      username = $remember.username;
    }
  });

  async function handleSubmit(event: SubmitEvent) {
    event.preventDefault();
    errorMessage = '';
    usernameError = '';
    passwordError = '';

    // Validate
    if (!username) {
      usernameError = 'Username is required';
      return;
    }
    if (!password || password.length < 4) {
      passwordError = 'Password must be at least 4 characters';
      return;
    }

    // Remember username if checked
    if ($remember.rememberMe) {
      $remember.username = username;
    } else {
      $remember.username = '';
    }

    isLoading = true;

    try {
      const baseUrl = env.PUBLIC_IGGY_API_URL || '';
      const response = await fetch(`${baseUrl}/users/login`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ username, password })
      });

      if (!response.ok) {
        errorMessage = 'Username or password is not valid';
        isLoading = false;
        return;
      }

      const data = await response.json();
      const { access_token } = data;

      authStore.login(access_token.token, access_token.expiry);
      goto(resolve(typedRoute('/dashboard/overview')));
    } catch (e) {
      errorMessage = 'Failed to connect to server';
      isLoading = false;
    }
  }
</script>

<form
  onsubmit={handleSubmit}
  class="min-w-[350px] max-w-[400px] bg-white dark:bg-shade-d700 border text-color p-5 rounded-2xl card-shadow dark:shadow-lg flex flex-col gap-5"
>
  <span class="mx-auto font-semibold">Admin sign in</span>

  <Input
    label="Username"
    name="username"
    autocomplete="username"
    errorMessage={usernameError}
    bind:value={username}
    required
  />

  <PasswordInput
    label="Password"
    name="password"
    autocomplete="current-password"
    errorMessage={passwordError}
    bind:value={password}
    required
    minlength={4}
  />

  {#if errorMessage}
    <span class="text-sm mx-auto text-red-500">{errorMessage}</span>
  {/if}

  <div class="flex justify-between items-center">
    <label class="flex gap-1 items-center w-fit hover:cursor-pointer">
      <Checkbox bind:checked={$remember.rememberMe} value="rememberMe" />
      <span class="text-xs font-light">Remember me</span>
    </label>
  </div>

  <Button variant="contained" size="lg" class="mt-7" type="submit" disabled={isLoading}>
    <span>{isLoading ? 'Logging in...' : 'Login'}</span>
    <Icon name="login" class="w-[23px] h-[23px]" />
  </Button>

  <span></span>
</form>
