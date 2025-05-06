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
