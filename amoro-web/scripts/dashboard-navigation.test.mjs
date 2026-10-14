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

import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
import test from 'node:test'
import { createContext, SourceTextModule, SyntheticModule } from 'node:vm'
import ts from 'typescript'
import { createMemoryHistory, createRouter } from 'vue-router'

// Load production modules, substituting only Store and browser history.
// https://nodejs.org/docs/latest-v22.x/api/vm.html#class-vmsourcetextmodule
// https://github.com/microsoft/TypeScript/wiki/Using-the-Compiler-API#a-simple-transform-function
const context = createContext({})
const store = { userInfo: { privileges: [] } }
const storeModule = new SyntheticModule(['default'], function () {
  this.setExport('default', () => store)
}, { context })
const routerModule = new SyntheticModule(['createRouter', 'createWebHashHistory'], function () {
  this.setExport('createRouter', createRouter)
  this.setExport('createWebHashHistory', createMemoryHistory)
}, { context })

async function loadModule(path, dependencies) {
  const source = readFileSync(new URL(path, import.meta.url), 'utf8')
  const { outputText } = ts.transpileModule(source, {
    compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ESNext },
  })
  const module = new SourceTextModule(outputText, { context })
  await module.link((specifier) => {
    assert.ok(dependencies[specifier], `Unexpected production dependency: ${specifier}`)
    return dependencies[specifier]
  })
  await module.evaluate()
  return module.namespace
}

const permissions = await loadModule('../src/utils/permission.ts', { '@/store': storeModule })
const { default: router } = await loadModule('../src/router/index.ts', { 'vue-router': routerModule })

test('removed Terminal URLs resolve to the existing not-found page', () => {
  // https://router.vuejs.org/guide/essentials/dynamic-matching.html#catch-all-404-not-found-route
  assert.equal(router.resolve('/terminal').name, 'Page404')
  assert.equal(router.resolve('/terminal/').name, 'Page404')
  assert.equal(router.hasRoute('Terminal'), false)
})

test('retained management routes preserve their route names', () => {
  for (const [path, name] of Object.entries({
    '/tables': 'Tables', '/catalogs': 'Catalogs', '/optimizing': 'Optimizing',
    '/overview': 'Overview', '/settings': 'Settings', '/login': 'Login',
  })) {
    assert.equal(router.resolve(path).name, name, path)
  }
})

test('default navigation preserves management privilege priorities', () => {
  const cases = [
    [[], '/login'],
    [['VIEW_TABLE', 'VIEW_CATALOG', 'VIEW_OPTIMIZER', 'VIEW_SYSTEM', 'MANAGE_PLATFORM'], '/tables'],
    [['VIEW_CATALOG', 'VIEW_OPTIMIZER', 'VIEW_SYSTEM', 'MANAGE_PLATFORM'], '/catalogs'],
    [['VIEW_OPTIMIZER', 'VIEW_SYSTEM', 'MANAGE_PLATFORM'], '/optimizing'],
    [['VIEW_SYSTEM', 'MANAGE_PLATFORM'], '/overview'],
    [['MANAGE_PLATFORM'], '/settings'],
    [['MANAGE_TABLE'], '/login'],
  ]
  for (const [privileges, expected] of cases) {
    store.userInfo.privileges = privileges
    assert.equal(permissions.getDefaultRoute(), expected, privileges.join(','))
  }
})

test('legacy SQL privilege cannot restore a removed default route', () => {
  store.userInfo.privileges = ['EXECUTE_SQL']
  assert.equal(permissions.getDefaultRoute(), '/login')
  store.userInfo.privileges = ['EXECUTE_SQL', 'MANAGE_PLATFORM']
  assert.equal(permissions.getDefaultRoute(), '/settings')
})
