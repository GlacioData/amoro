/*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import antfu from '@antfu/eslint-config'

export default antfu({
  stylistic: {
    indent: 2,
    quotes: 'single',
  },

  // Keep the v2 import-order policy; this migration does not reorder source imports.
  // Source: https://github.com/antfu/eslint-config/blob/v2.21.1/src/configs/perfectionist.ts
  perfectionist: {
    overrides: {
      // v9 replaced import/order with perfectionist; retain group-only ordering.
      // Source: https://perfectionist.dev/rules/sort-imports#type
      'perfectionist/sort-imports': ['error', {
        type: 'unsorted',
        groups: ['builtin', 'external', 'parent', 'sibling', 'index', ['internal', 'unknown']],
        internalPattern: ['^@/'],
        newlinesBetween: 'ignore',
        newlinesInside: 'ignore',
      }],
      'perfectionist/sort-named-imports': 'off',
    },
  },

  // Match the existing policy for intentionally ignored catch parameters.
  // Other unused bindings remain checked; vue-tsc also retains noUnusedLocals.
  rules: {
    'sort-imports': ['error', { ignoreDeclarationSort: true }],
    'unused-imports/no-unused-vars': ['error', {
      args: 'after-used',
      argsIgnorePattern: '^_',
      caughtErrors: 'none',
      ignoreRestSiblings: true,
      vars: 'all',
      varsIgnorePattern: '^_',
    }],
  },

  typescript: true,
  vue: {
    overrides: {
      'unused-imports/no-unused-vars': ['error', {
        args: 'after-used',
        argsIgnorePattern: '^_',
        caughtErrors: 'none',
        ignoreRestSiblings: true,
        vars: 'all',
        varsIgnorePattern: '^_',
      }],
    },
  },

  jsonc: false,
  yaml: false,

  ignores: [
    '**/fixtures',
    '**/node_modules',
    '**/dist',
    'mock',
    '**/src/main',
    '**/license.node.js',
  ],
})
