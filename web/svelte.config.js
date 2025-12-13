/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import adapterNode from '@sveltejs/adapter-node';
import adapterStatic from '@sveltejs/adapter-static';
import { vitePreprocess } from '@sveltejs/vite-plugin-svelte';

// Use static adapter when STATIC_BUILD env is set (for embedding in Rust server)
const useStaticAdapter = process.env.STATIC_BUILD === 'true';

/** @type {import('@sveltejs/kit').Config} */
const config = {
  kit: {
    adapter: useStaticAdapter
      ? adapterStatic({
          pages: 'build/static',
          assets: 'build/static',
          fallback: 'index.html'
        })
      : adapterNode({
          out: 'build'
        }),
    paths: {
      base: useStaticAdapter ? '/ui' : ''
    },
    csrf: {
      trustedOrigins: ['*']
    }
  },
  preprocess: vitePreprocess()
};
export default config;
