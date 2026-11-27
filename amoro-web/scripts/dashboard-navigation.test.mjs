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

test('removed Terminal and Settings URLs resolve to the existing not-found page', () => {
  // https://router.vuejs.org/guide/essentials/dynamic-matching.html#catch-all-404-not-found-route
  for (const [path, name] of [['/terminal', 'Terminal'], ['/settings', 'Settings']]) {
    assert.equal(router.resolve(path).name, 'Page404')
    assert.equal(router.resolve(`${path}/`).name, 'Page404')
    assert.equal(router.hasRoute(name), false)
  }
})

test('removed unfinished Create table route resolves to the existing not-found page', () => {
  assert.equal(router.resolve('/tables/create').name, 'Page404')
  assert.equal(router.resolve('/tables/create/').name, 'Page404')
  assert.equal(router.hasRoute('Create'), false)
})

test('retained management routes preserve their route names', () => {
  for (const [path, name] of Object.entries({
    '/tables': 'Tables', '/catalogs': 'Catalogs', '/optimizing': 'Optimizing',
    '/overview': 'Overview', '/login': 'Login',
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
    [['MANAGE_PLATFORM'], '/login'],
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
  assert.equal(permissions.getDefaultRoute(), '/login')
})

test('capturePreLoginTarget captures unauthenticated hash deep links and 403 origin without clobbering', () => {
  const deepLink = permissions.capturePreLoginTarget(
    { path: '/catalogs', query: { catalogname: 'test', type: 'hive' } },
    { path: '/', query: {} },
    false,
  )
  assert.equal(deepLink?.path, '/catalogs')
  assert.equal(deepLink?.query?.catalogname, 'test')
  assert.equal(deepLink?.query?.type, 'hive')

  // Redirecting to /login from START_LOCATION ('/') must return null so deepLink is preserved.
  assert.equal(
    permissions.capturePreLoginTarget({ path: '/login', query: {} }, { path: '/', query: {} }, false),
    null,
  )

  // Session expiry (403) pushing to /login from an active page captures the active page and query.
  const expiredFrom = permissions.capturePreLoginTarget(
    { path: '/login', query: {} },
    { path: '/tables', query: { catalog: 'c1', db: 'd1', table: 't1', tab: 'Files' } },
    false,
  )
  assert.equal(expiredFrom?.path, '/tables')
  assert.equal(expiredFrom?.query?.tab, 'Files')

  // Normal authenticated navigation does not record a pre-login target.
  assert.equal(
    permissions.capturePreLoginTarget({ path: '/overview', query: {} }, { path: '/tables', query: {} }, true),
    null,
  )
})

test('resolvePostLoginTarget restores permitted internal routes and rejects open redirects', () => {
  store.userInfo.privileges = ['VIEW_TABLE', 'VIEW_CATALOG', 'VIEW_SYSTEM']
  const restored = permissions.resolvePostLoginTarget({
    path: '/catalogs',
    query: { catalogname: 'lake' },
  })
  assert.equal(restored.path, '/catalogs')
  assert.equal(restored.query.catalogname, 'lake')

  // User without VIEW_CATALOG falls back to default permitted route (/tables).
  store.userInfo.privileges = ['VIEW_TABLE']
  const fallbackNoPerm = permissions.resolvePostLoginTarget({
    path: '/catalogs',
    query: { catalogname: 'lake' },
  })
  assert.equal(fallbackNoPerm.path, '/tables')

  // Open redirect payloads and /login must always fall back to default route.
  store.userInfo.privileges = ['VIEW_TABLE', 'VIEW_CATALOG']
  for (const unsafe of ['//evil.com', 'https://evil.com/x', 'http://evil.com', '/\\evil.com', '/login', '/login/', '/LOGIN', '/', '/404/']) {
    const resolved = permissions.resolvePostLoginTarget({ path: unsafe, query: { a: '1' } })
    assert.equal(resolved.path, '/tables', `Unsafe target should be rejected: ${unsafe}`)
  }
})

test('canAccessRoute enforces per-route privileges including trailing-slash and mixed-case paths', () => {
  // VIEW_TABLE alone cannot access hive-upgrade (requires MANAGE_TABLE) or catalogs/overview/optimizing
  store.userInfo.privileges = ['VIEW_TABLE']
  assert.equal(permissions.canAccessRoute('/tables'), true)
  assert.equal(permissions.canAccessRoute('/tables/'), true)
  assert.equal(permissions.canAccessRoute('/tables/hive-upgrade'), false)
  assert.equal(permissions.canAccessRoute('/tables/hive-upgrade/'), false)
  assert.equal(permissions.canAccessRoute('/Tables/Hive-Upgrade/'), false)
  assert.equal(permissions.canAccessRoute('/hive-tables/upgrade/'), false)
  assert.equal(permissions.canAccessRoute('/catalogs/'), false)
  assert.equal(permissions.canAccessRoute('/CATALOGS'), false)
  assert.equal(permissions.canAccessRoute('/overview/'), false)
  assert.equal(permissions.canAccessRoute('/optimizing/'), false)

  // Granting MANAGE_TABLE permits hive-upgrade
  store.userInfo.privileges = ['VIEW_TABLE', 'MANAGE_TABLE']
  assert.equal(permissions.canAccessRoute('/tables/hive-upgrade'), true)
  assert.equal(permissions.canAccessRoute('/tables/hive-upgrade/'), true)
  assert.equal(permissions.canAccessRoute('/hive-tables/upgrade/'), true)
})
