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
import { existsSync, readdirSync, readFileSync, statSync } from 'node:fs'
import path from 'node:path'
import test from 'node:test'
import { fileURLToPath } from 'node:url'

const webRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), '..')
const srcRoot = path.join(webRoot, 'src')

function collectSourceFiles(dir: string, out: string[] = []): string[] {
  for (const entry of readdirSync(dir, { withFileTypes: true })) {
    if (dir === srcRoot && entry.name === 'main')
      continue
    const full = path.join(dir, entry.name)
    if (entry.isDirectory())
      collectSourceFiles(full, out)
    else if (/\.(?:vue|ts)$/.test(entry.name) && !entry.name.endsWith('.d.ts'))
      out.push(full)
  }
  return out
}

function resolveLocalImport(fromFile: string, specifier: string): string | null {
  let base: string
  if (specifier.startsWith('@/'))
    base = path.join(srcRoot, specifier.slice(2))
  else if (specifier.startsWith('.'))
    base = path.resolve(path.dirname(fromFile), specifier)
  else
    return null

  const cleanBase = base.split('?')[0]
  for (const ext of ['.ts', '.vue', '.less', '.svg', '']) {
    const direct = cleanBase + ext
    if (existsSync(direct) && statSync(direct).isFile())
      return direct
    const indexFile = path.join(cleanBase, `index${ext}`)
    if (existsSync(indexFile) && statSync(indexFile).isFile())
      return indexFile
  }
  return null
}

test('every Vue and TypeScript module under src/ is reachable from src/main.ts', () => {
  const allFiles = collectSourceFiles(srcRoot)
  const visited = new Set<string>()
  const queue = [path.join(srcRoot, 'main.ts')]

  while (queue.length) {
    const current = queue.shift()
    if (!current || visited.has(current) || !existsSync(current))
      continue
    visited.add(current)
    const stripped = readFileSync(current, 'utf8')
      .replace(/\/\*[\s\S]*?\*\//g, '')
      .replace(/^\s*\/\/.*$/gm, '')
    for (const match of stripped.matchAll(/(?:from|import)\s*\(?['"]([^'"]+)['"]/g)) {
      const target = resolveLocalImport(current, match[1])
      if (target && !visited.has(target))
        queue.push(target)
    }
  }

  const orphans = allFiles
    .filter(file => !visited.has(file))
    .map(file => path.relative(srcRoot, file))
  assert.deepEqual(orphans, [])
})

test('direct package.json dependencies match licenses/LICENSE-* files 1:1', () => {
  const pkg = JSON.parse(readFileSync(path.join(webRoot, 'package.json'), 'utf8')) as {
    dependencies?: Record<string, string>
    devDependencies?: Record<string, string>
  }
  const expected = new Set(
    [...Object.keys(pkg.dependencies ?? {}), ...Object.keys(pkg.devDependencies ?? {})]
      .map(name => `LICENSE-${name.replace(/\//g, '-')}`),
  )
  const actual = new Set(
    readdirSync(path.join(webRoot, 'licenses')).filter(name => name.startsWith('LICENSE-')),
  )
  const missing = [...expected].filter(name => !actual.has(name)).sort()
  const stale = [...actual].filter(name => !expected.has(name)).sort()
  assert.deepEqual({ missing, stale }, { missing: [], stale: [] })
})

test('en.ts and zh.ts define identical translation key sets', () => {
  const extractKeys = (file: string) => new Set(
    [...readFileSync(path.join(srcRoot, 'language', file), 'utf8').matchAll(/^\s*['"]?([A-Z0-9_]+)['"]?\s*:/gim)]
      .map(match => match[1]),
  )
  const en = extractKeys('en.ts')
  const zh = extractKeys('zh.ts')
  const missingInZh = [...en].filter(key => !zh.has(key)).sort()
  const missingInEn = [...zh].filter(key => !en.has(key)).sort()
  assert.deepEqual({ missingInZh, missingInEn }, { missingInZh: [], missingInEn: [] })
})

test('no legacy Ant Design Vue props, typos, or unscoped global input pollution remain', () => {
  const files = collectSourceFiles(srcRoot)
  const violations: string[] = []

  for (const file of files) {
    const rel = path.relative(srcRoot, file)
    const content = readFileSync(file, 'utf8')

    if (/show-upload-list/.test(content))
      violations.push(`${rel}: uses legacy AntD show-upload-list instead of show-file-list`)
    if (/type="ghost"/.test(content))
      violations.push(`${rel}: uses invalid Element Plus button type="ghost"`)
    if (/placement="(?:topRight|topLeft|bottomRight|bottomLeft)"/.test(content))
      violations.push(`${rel}: uses legacy AntD placement instead of Element Plus popper placement`)
    if (/\$t\(\s*placeholder\./.test(content))
      violations.push(`${rel}: double-translates already translated placeholder value`)
    if (/\blable\s*:/.test(content))
      violations.push(`${rel}: contains "lable" option typo instead of "label"`)
    if (/\.(?:table-failed-tip|topbar-tooltip)\s+\.el-popper|\.(?:table-failed-tip|topbar-tooltip)\s*\{\s*\.el-popper/.test(content))
      violations.push(`${rel}: popper-class targets descendant .el-popper instead of .el-popper.<class>`)
  }

  const hiveProps = readFileSync(path.join(srcRoot, 'views/hive-details/components/Properties.vue'), 'utf8')
  if (/<style\s+lang="less"\s*>/.test(hiveProps))
    violations.push('views/hive-details/components/Properties.vue: <style> must be scoped to avoid global .config-properties/.el-input__inner pollution')

  assert.deepEqual(violations, [])
})

test('all static SVG icon names exist in sprite.svg', () => {
  const sprite = readFileSync(path.join(srcRoot, 'assets/icons/sprite.svg'), 'utf8')
  const symbols = new Set([...sprite.matchAll(/id="icon-([^"]+)"/g)].map(match => match[1]))

  const missing: string[] = []
  for (const file of collectSourceFiles(srcRoot)) {
    const rel = path.relative(srcRoot, file)
    const content = readFileSync(file, 'utf8')
    for (const match of content.matchAll(/(?<!:)icon-class="([^"]+)"/g)) {
      if (!symbols.has(match[1]))
        missing.push(`${rel}: icon-class="${match[1]}"`)
    }
  }

  const sidebar = readFileSync(path.join(srcRoot, 'components/Sidebar.vue'), 'utf8')
  for (const match of sidebar.matchAll(/icon:\s*'([^']+)'/g)) {
    if (!symbols.has(match[1]))
      missing.push(`components/Sidebar.vue: icon '${match[1]}'`)
  }

  assert.deepEqual(missing, [])
})

test('all table detail sub-tabs bind :key="tableKey" to prevent stale cross-table data', () => {
  const tablesView = readFileSync(path.join(srcRoot, 'views/tables/index.vue'), 'utf8')
  assert.match(tablesView, /<UDetails[^>]*:key="tableKey"/)
  assert.match(tablesView, /<UFiles[^>]*:key="tableKey"/)
  assert.match(tablesView, /<USnapshots[\s\S]*?:key="tableKey"/)
  assert.match(tablesView, /<UTags[^>]*:key="tableKey"/)
  assert.match(tablesView, /<component[^>]*:is="`U\$\{tab\.key\}`"[^>]*:key="tableKey"/)
})

test('table subviews avoid known layout, row-key, tooltip, and breadcrumb click regressions', () => {
  const filesView = readFileSync(path.join(srcRoot, 'views/tables/components/Files.vue'), 'utf8')
  const operationsView = readFileSync(path.join(srcRoot, 'views/tables/components/Operations.vue'), 'utf8')
  const optimizingView = readFileSync(path.join(srcRoot, 'views/tables/components/Optimizing.vue'), 'utf8')
  const legacySnapshotsView = readFileSync(path.join(srcRoot, 'views/tables/components/LegacySnapshots.vue'), 'utf8')
  const selectorView = readFileSync(path.join(srcRoot, 'views/tables/components/Selector.vue'), 'utf8')
  const catalogsView = readFileSync(path.join(srcRoot, 'views/catalogs/index.vue'), 'utf8')

  assert.doesNotMatch(filesView, /\.el-input-group__append\)\s*\{\s*display:\s*none/)
  assert.doesNotMatch(operationsView, /row-key="partiton"/)
  assert.match(operationsView, /white-space:\s*pre-wrap/)
  assert.doesNotMatch(optimizingView, /flex:\s*'0 0 420px'/)
  assert.doesNotMatch(optimizingView, /@click="toggleBreadcrumb"/)
  assert.doesNotMatch(optimizingView, /<el-tooltip>[\s\S]*?formatPartition\(record\.partitionData\)/)
  assert.doesNotMatch(legacySnapshotsView, /@click="toggleBreadcrumb"/)
  assert.doesNotMatch(selectorView, /layout="prev, pager, next, jumper"/)
  assert.match(catalogsView, /:title="item\.catalogName"/)
})

test('TableExplorer.vue internationalizes all user-visible labels, tooltips, and status copy', () => {
  const explorer = readFileSync(path.join(srcRoot, 'views/tables/components/TableExplorer.vue'), 'utf8')
  const templateMatch = explorer.match(/<template>([\s\S]*?)<\/template>/)
  assert.ok(templateMatch, 'TableExplorer.vue must contain a <template> block')
  const template = templateMatch[1]

  assert.match(explorer, /useI18n\(\)/)
  for (const literal of [
    'Search catalog.database.table',
    'Refresh catalogs and open branches',
    'Searches loaded branches',
    'Could not load catalogs',
    'Could not finish searching',
    'Retry loading',
    'Loading catalogs',
    'Searching…',
    'No results',
    'No catalogs',
    '>Empty<',
    '>\n        Retry\n',
  ]) {
    assert.equal(template.includes(literal), false, `Hardcoded English string found in TableExplorer.vue: ${literal}`)
  }
})

test('reset.less, login/index.vue, and main.ts satisfy accessibility, security, and CSS validity contracts', () => {
  const resetLess = readFileSync(path.join(srcRoot, 'styles/reset.less'), 'utf8')
  const loginView = readFileSync(path.join(srcRoot, 'views/login/index.vue'), 'utf8')
  const mainTs = readFileSync(path.join(srcRoot, 'main.ts'), 'utf8')

  assert.doesNotMatch(resetLess, /text-decoration:\s*none\s*!important/)
  assert.doesNotMatch(loginView, /font:\s*14px\/44px\s*!important/)
  assert.match(loginView, /<button[^>]*type="button"[^>]*class="password-toggle"/)
  assert.doesNotMatch(loginView, /window\.location\.href\s*=\s*['"]\/['"]/)
  assert.doesNotMatch(mainTs, /window\.location\.pathname/)

  for (const match of loginView.matchAll(/<a\b[^>]*target="_blank"[^>]*>/g)) {
    assert.match(match[0], /rel="noopener noreferrer"/, `External link missing rel="noopener noreferrer": ${match[0]}`)
  }
})

test('vue-router 5.5 navigation guards avoid deprecated next() callback and chart labels check translation existence', () => {
  const catalogsView = readFileSync(path.join(srcRoot, 'views/catalogs/index.vue'), 'utf8')
  const chartUtil = readFileSync(path.join(srcRoot, 'utils/chart.ts'), 'utf8')

  assert.doesNotMatch(catalogsView, /onBeforeRouteLeave\(\s*\([^)]*,\s*[^)]*,\s*next\s*\)/)
  assert.match(chartUtil, /i18n\.global\.te\(/)
})

test('TableExplorer revealSelectedPath never auto-expands database nodes or eagerly fetches table lists', () => {
  const explorer = readFileSync(path.join(srcRoot, 'views/tables/components/TableExplorer.vue'), 'utf8')
  const match = explorer.match(/async function revealSelectedPath\(\)\s*\{[\s\S]*?\n\}/)
  assert.ok(match, 'TableExplorer.vue must define revealSelectedPath()')
  const body = match[0]

  assert.doesNotMatch(body, /setBrowseExpanded\(\s*database\.key/)
  assert.doesNotMatch(body, /loadChildren\(\s*database\s*\)/)
})

test('Optimizing and Process tables use adaptive layout, non-shrinking status indicators, and compatible header styling', () => {
  const optimizingView = readFileSync(path.join(srcRoot, 'views/tables/components/Optimizing.vue'), 'utf8')
  const processView = readFileSync(path.join(srcRoot, 'views/tables/components/Process.vue'), 'utf8')
  const elementPlusLess = readFileSync(path.join(srcRoot, 'styles/element-plus.less'), 'utf8')

  assert.match(optimizingView, /table-layout="auto"/)
  assert.doesNotMatch(optimizingView, /:deep\(\.el-table__header > tr > th\)/)
  assert.match(optimizingView, /\.status-icon\s*\{[\s\S]*?(?:flex:\s*none|flex-shrink:\s*0)/)
  assert.match(optimizingView, /buildOptimizingProcessColumns/)
  assert.match(optimizingView, /buildOptimizingTaskColumns/)

  assert.doesNotMatch(processView, /:deep\(\.el-table__header > tr > th\)/)
  assert.match(processView, /\.status-icon\s*\{[\s\S]*?(?:flex:\s*none|flex-shrink:\s*0)/)
  assert.match(processView, /buildProcessColumns/)

  assert.doesNotMatch(elementPlusLess, /\.el-table__header\s+th\.el-table__cell/)
  assert.match(elementPlusLess, /th\.el-table__cell\s*\{\s*font-weight:\s*600;/)
})
