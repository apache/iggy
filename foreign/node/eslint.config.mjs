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

import { builtinModules } from "module"

// import jsLint from "@eslint/js"
// import stylistic from "@stylistic/eslint-plugin"
// import globals from "globals"
import tsLint from "typescript-eslint"

export default [
  // config parsers
  {
    files: ["**/*.{js,mjs,cjs,ts}"]
  },
  {
    languageOptions: {
      // globals: {
      //   ...globals.node
      // }
    }
  },
  // rules
  // jsLint.configs.recommended,
  ...tsLint.configs.recommended,
  {
    rules: {
      "@typescript-eslint/consistent-type-imports": [
        "error",
        {
          prefer: "type-imports",
          fixStyle: "separate-type-imports"
        }
      ]
    }
  },
  {
    plugins: {

    },
  },

  // see: https://eslint.style/guide/getting-started
  // see: https://github.com/eslint-stylistic/eslint-stylistic/blob/main/packages/eslint-plugin/configs/disable-legacy.ts
  // stylistic.configs["disable-legacy"],
  // stylistic.configs.customize({
  //   indent: 4,
  //   quotes: "double",
  //   semi: false,
  //   commaDangle: "never",
  //   jsx: true
  // }),

  {
    // https://eslint.org/docs/latest/use/configure/ignore
    ignores: [
      "node_modules",
      "dist",
    ]
  }
]
