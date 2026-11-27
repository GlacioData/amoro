<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
/ -->

<script setup lang="ts">
import type { TreeInstance } from 'element-plus'
import { CaretRight, Loading, Refresh, Search } from '@element-plus/icons-vue'
import { computed, markRaw, nextTick, onBeforeMount, onBeforeUnmount, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import {
  createBranchLoader,
  createTableSearchPlan,
  getRevealScrollKey,
  getSelectedPathExpandedKeys,
  getStoredCatalogExpandedKeys,
  mergeExpandedKeys,
  reconcileTreeNodes,
} from './table-explorer-state'
import spriteUrl from '@/assets/icons/sprite.svg?no-inline'
import { getCatalogList, getDatabaseList, getTableList } from '@/services/table.service'
import { tableTypeIconMap } from '@/types/common.type'
import type { ICatalogItem } from '@/types/common.type'

// Node types: Catalog / Database / Table
type NodeType = 'catalog' | 'database' | 'table'

interface TableItem {
  name: string
  type: string
}

interface TreeNode {
  key: string
  title: string
  children?: TreeNode[]
  // Custom fields
  nodeType: NodeType
  catalog: string
  db?: string
  table?: string
  tableType?: string
}

const { t } = useI18n()
const router = useRouter()
const route = useRoute()

const storageTableKey = 'easylake-menu-catalog-db-table'
const expandedKeysSessionKey = 'tables_expanded_keys'
const requestOptions = { handleError: false }
const requestBranch = createBranchLoader<boolean>()
const loadedRevisions = new WeakMap<TreeNode, number>()
const branchStatus = reactive(new Map<string, 'loading' | 'error'>())
const searchExpansion = reactive(new Map<string, boolean>())
const treeVersion = ref(0)
const rootStatus = ref<'loading' | 'error' | 'ready'>('loading')
const searchStatus = ref<'idle' | 'loading' | 'error'>('idle')
const treeBody = ref<HTMLElement>()
let refreshRevision = 0
let searchRequestId = 0
let disposed = false
let browseScrollTop = 0

const state = reactive({
  refreshing: false,
  searchKey: '',
  filterKey: '',
  treeData: [] as TreeNode[],
  expandedKeys: [] as string[],
  searchTablesByDatabase: {} as Record<string, TreeNode[]>,
})

function buildCatalogNode(catalog: string): TreeNode {
  return {
    key: `catalog:${catalog}`,
    title: catalog,
    nodeType: 'catalog',
    catalog,
  }
}

function buildDatabaseNode(catalog: string, db: string): TreeNode {
  return {
    key: `catalog:${catalog}/db:${db}`,
    title: db,
    nodeType: 'database',
    catalog,
    db,
  }
}

function buildTableNode(catalog: string, db: string, table: TableItem): TreeNode {
  // Keep large, immutable leaf lists out of Vue's deep reactivity.
  // https://vuejs.org/guide/best-practices/performance.html#reduce-reactivity-overhead-for-large-immutable-structures
  return markRaw({
    key: `catalog:${catalog}/db:${db}/table:${table.name}`,
    title: table.name,
    nodeType: 'table',
    catalog,
    db,
    table: table.name,
    tableType: table.type,
  })
}

function findCatalog(catalog: string) {
  return state.treeData.find(node => node.catalog === catalog)
}

function findBranch(node: TreeNode) {
  const catalog = findCatalog(node.catalog)
  return node.nodeType === 'catalog'
    ? catalog
    : catalog?.children?.find(child => child.key === node.key)
}

function loadCatalogs() {
  return requestBranch('catalogs', refreshRevision, async () => {
    rootStatus.value = 'loading'
    try {
      const catalogs = await getCatalogList(requestOptions)
      if (disposed)
        return false
      state.treeData = reconcileTreeNodes(
        state.treeData,
        (catalogs || []).map((item: ICatalogItem) => buildCatalogNode(item.catalogName)),
      )
      rootStatus.value = 'ready'
      return true
    }
    catch {
      rootStatus.value = 'error'
      return false
    }
  })
}

async function fetchChildren(node: TreeNode): Promise<TreeNode[]> {
  const { catalog, db } = node
  if (node.nodeType === 'catalog') {
    const databases = await getDatabaseList({ catalog, keywords: '' }, requestOptions)
    return (databases || []).map(database => buildDatabaseNode(catalog, database))
  }
  if (!db)
    return []
  const tables: TableItem[] = await getTableList({ catalog, db, keywords: '' }, requestOptions) || []
  return tables.map(table => buildTableNode(catalog, db, table))
}

function loadChildren(source: TreeNode): Promise<boolean> {
  const node = findBranch(source)
  const revision = refreshRevision
  if (!node)
    return Promise.resolve(false)
  if (loadedRevisions.get(node) === revision)
    return Promise.resolve(true)

  return requestBranch(node.key, revision, async () => {
    const isCurrent = () => !disposed && revision === refreshRevision && findBranch(node) === node
    if (!isCurrent())
      return false

    branchStatus.set(node.key, 'loading')
    try {
      const children = await fetchChildren(node)
      if (!isCurrent())
        return false

      const updated = reconcileTreeNodes(node.children, children)
      if (updated !== node.children) {
        node.children = updated
        treeVersion.value += 1
      }
      loadedRevisions.set(node, revision)
      branchStatus.delete(node.key)
      return true
    }
    catch {
      if (isCurrent())
        branchStatus.set(node.key, 'error')
      return false
    }
  })
}

function persistBrowseExpandedKeys(nextKeys: string[]) {
  state.expandedKeys = nextKeys
  try {
    sessionStorage.setItem(expandedKeysSessionKey, JSON.stringify(state.expandedKeys))
  }
  catch {
    // Browser storage is optional for embedders.
  }
}

function setBrowseExpanded(key: string, expanded: boolean) {
  const keys = state.expandedKeys
  if (expanded === keys.includes(key))
    return
  persistBrowseExpandedKeys(expanded ? [...keys, key] : keys.filter(item => item !== key))
}

function handleSelectTable(catalog: string, db: string, tableName: string, tableType: string) {
  if (!catalog || !db || !tableName) {
    return
  }

  const type = tableType || 'MIXED_ICEBERG'

  localStorage.setItem(storageTableKey, JSON.stringify({
    catalog,
    database: db,
    tableName,
    type,
  }))

  const pathQuery = {
    path: '/tables',
    query: {
      catalog,
      db,
      table: tableName,
      type,
    },
  }

  if (route.path.includes('tables')) {
    router.replace(pathQuery)
  }
  else {
    router.push(pathQuery)
  }
}

const treeRef = ref<TreeInstance>()

function handleTreeSelect(node: TreeNode) {
  if (node.nodeType !== 'table') {
    syncCurrentTable()
    toggleNodeExpand(node)
    return
  }
  handleSelectTable(node.catalog, node.db || '', node.table || '', node.tableType || '')
}

// Preserve the dashboard's directional navigation and Enter/Space activation.
function handleTreeKeydown(event: KeyboardEvent) {
  if (!['ArrowLeft', 'ArrowRight', 'Enter', ' '].includes(event.key))
    return
  const target = event.target as HTMLElement
  if (target.closest('button'))
    return
  const row = target.closest<HTMLElement>('[role="treeitem"]')
  const node = row?.dataset.key ? treeRef.value?.getNode(row.dataset.key) : undefined
  if (!node)
    return
  event.preventDefault()
  event.stopPropagation()

  const data = node.data as TreeNode
  if (event.key === 'Enter' || event.key === ' ') {
    handleTreeSelect(data)
    return
  }

  const opening = event.key === 'ArrowRight'
  if (data.nodeType !== 'table' && node.expanded !== opening) {
    setTreeExpanded(data, opening)
    return
  }

  const nextKey = opening ? node.childNodes[0]?.key : node.level > 1 ? node.parent?.key : undefined
  if (nextKey != null)
    findTreeRow(String(nextKey))?.focus()
}

function findTreeRow(key: string) {
  return treeBody.value?.querySelector<HTMLElement>(`[role="treeitem"][data-key="${CSS.escape(key)}"]`)
}

function getNodeIcon(node: TreeNode) {
  if (node.nodeType === 'catalog') {
    return 'catalogs'
  }
  if (node.nodeType === 'database') {
    return 'database'
  }
  return tableTypeIconMap[node.tableType as keyof typeof tableTypeIconMap] || 'tables'
}

function normalizeKeyword(raw: string) {
  return raw.trim().toLowerCase()
}

function getSearchTreeSource(): TreeNode[] {
  if (!Object.keys(state.searchTablesByDatabase).length) {
    return state.treeData
  }

  return state.treeData.map((catalogNode) => {
    if (!catalogNode.children?.length) {
      return catalogNode
    }

    let catalogChanged = false
    const children = catalogNode.children.map((databaseNode) => {
      if (!Object.hasOwn(state.searchTablesByDatabase, databaseNode.key)) {
        return databaseNode
      }

      catalogChanged = true
      return {
        ...databaseNode,
        children: state.searchTablesByDatabase[databaseNode.key],
      }
    })

    return catalogChanged ? { ...catalogNode, children } : catalogNode
  })
}

function filterBySingleKeyword(nodes: TreeNode[], keyword: string, expandedSet: Set<string>): TreeNode[] {
  const result: TreeNode[] = []

  nodes.forEach((node) => {
    const titleMatch = node.title.toLowerCase().includes(keyword)
    let childrenMatches: TreeNode[] = []

    if (node.children && node.children.length) {
      childrenMatches = filterBySingleKeyword(node.children, keyword, expandedSet)
    }

    if (titleMatch || childrenMatches.length) {
      const cloned: TreeNode = { ...node }
      if (childrenMatches.length) {
        cloned.children = childrenMatches
        expandedSet.add(node.key)
      }
      result.push(cloned)
    }
  })

  return result
}

function filterByHierarchical(nodes: TreeNode[], parts: string[], expandedSet: Set<string>): TreeNode[] {
  const [catalogPart, dbPart, tablePart] = parts
  const result: TreeNode[] = []

  nodes.forEach((catalogNode) => {
    if (catalogNode.nodeType !== 'catalog') {
      return
    }
    if (!catalogNode.title.toLowerCase().includes(catalogPart)) {
      return
    }

    const dbChildren = (catalogNode.children || []).filter(child => child.nodeType === 'database')
    const matchedDbNodes: TreeNode[] = []

    dbChildren.forEach((dbNode) => {
      if (dbPart && !dbNode.title.toLowerCase().includes(dbPart)) {
        return
      }

      if (!tablePart) {
        const clonedDb: TreeNode = { ...dbNode }
        clonedDb.children = dbNode.children
        matchedDbNodes.push(clonedDb)
        expandedSet.add(catalogNode.key)
        expandedSet.add(dbNode.key)
        return
      }

      const tableChildren = (dbNode.children || []).filter(child => child.title.toLowerCase().includes(tablePart))
      if (tableChildren.length) {
        const clonedDb: TreeNode = { ...dbNode, children: tableChildren }
        matchedDbNodes.push(clonedDb)
        expandedSet.add(catalogNode.key)
        expandedSet.add(dbNode.key)
      }
    })

    if (matchedDbNodes.length) {
      const clonedCatalog: TreeNode = { ...catalogNode, children: matchedDbNodes }
      result.push(clonedCatalog)
    }
  })

  return result
}

function filterTree(source: TreeNode[], rawKeyword: string): { tree: TreeNode[], expandedKeys: string[] } {
  const keyword = normalizeKeyword(rawKeyword)
  if (!keyword) {
    return {
      tree: source,
      expandedKeys: state.expandedKeys,
    }
  }

  const expandedSet = new Set<string>()
  const parts = keyword.split('.').map(p => p.trim()).filter(Boolean)

  let filteredTree: TreeNode[] = []
  if (parts.length > 1) {
    filteredTree = filterByHierarchical(source, parts, expandedSet)
  }
  else {
    filteredTree = filterBySingleKeyword(source, keyword, expandedSet)
  }

  return {
    tree: filteredTree,
    expandedKeys: Array.from(expandedSet),
  }
}

let searchTimer: ReturnType<typeof setTimeout> | undefined

async function loadSearchTables(rawKeyword: string, keepResults = false) {
  const requestId = ++searchRequestId
  const { filterKeyword, target } = createTableSearchPlan(rawKeyword, {
    catalog: String(route.query.catalog || ''),
    database: String(route.query.db || ''),
  })
  const previousFilter = state.filterKey
  if (filterKeyword && !previousFilter)
    browseScrollTop = treeBody.value?.scrollTop || 0
  if (!keepResults || filterKeyword !== previousFilter) {
    state.searchTablesByDatabase = {}
    searchExpansion.clear()
  }
  state.filterKey = filterKeyword
  searchStatus.value = 'idle'
  if (!filterKeyword && previousFilter) {
    await nextTick()
    if (requestId === searchRequestId && treeBody.value)
      treeBody.value.scrollTop = browseScrollTop
  }

  const catalog = target && findCatalog(target.catalog)
  if (!target || !catalog)
    return

  searchStatus.value = 'loading'
  try {
    const loaded = await loadChildren(catalog)
    if (requestId !== searchRequestId)
      return
    if (!loaded)
      throw new Error('Unable to load databases')

    const database = catalog.children?.find(node => node.db === target.database)
    if (!database || loadedRevisions.get(database) === refreshRevision) {
      state.searchTablesByDatabase = {}
      return
    }

    const tables: TableItem[] = await getTableList({
      catalog: target.catalog,
      db: target.database,
      keywords: target.keyword,
    }, requestOptions) || []
    if (requestId === searchRequestId && findBranch(database) === database) {
      state.searchTablesByDatabase = {
        [database.key]: tables.map(table => buildTableNode(target.catalog, target.database, table)),
      }
    }
  }
  catch {
    if (requestId === searchRequestId)
      searchStatus.value = 'error'
  }
  finally {
    if (requestId === searchRequestId && searchStatus.value === 'loading')
      searchStatus.value = 'idle'
  }
}

function cancelScheduledSearch() {
  if (searchTimer !== undefined) {
    clearTimeout(searchTimer)
    searchTimer = undefined
  }
  searchRequestId += 1
}

watch(() => state.searchKey, (value) => {
  cancelScheduledSearch()
  if (!value.trim()) {
    void loadSearchTables('')
    return
  }
  searchTimer = setTimeout(() => {
    searchTimer = undefined
    void loadSearchTables(value)
  }, 500)
})

onBeforeUnmount(() => {
  disposed = true
  cancelScheduledSearch()
})

const selectedPath = computed(() => [
  String(route.query.catalog || ''),
  String(route.query.db || ''),
  String(route.query.table || ''),
] as const)
const selectedKey = computed(() => {
  const [catalog, db, table] = selectedPath.value
  return catalog && db && table ? `catalog:${catalog}/db:${db}/table:${table}` : null
})
const showSearchScope = computed(() => state.filterKey && !state.filterKey.includes('.'))

async function revealSelectedPath() {
  const path = selectedPath.value
  const [catalog, db, table] = path
  const catalogNode = catalog && findCatalog(catalog)
  if (!catalogNode || !db || state.filterKey)
    return

  const nextExpandedKeys = getSelectedPathExpandedKeys(state.expandedKeys, catalog)
  if (nextExpandedKeys !== state.expandedKeys)
    persistBrowseExpandedKeys(nextExpandedKeys)
  if (!await loadChildren(catalogNode) || state.filterKey || !isSelectedPath(path))
    return
  const database = catalogNode.children?.find(node => node.db === db)
  if (!database)
    return

  await nextTick()
  const targetKey = getRevealScrollKey(state.expandedKeys, catalog, db, table)
  const row = (targetKey ? findTreeRow(targetKey) : undefined) ?? findTreeRow(database.key)
  scrollSelectedRowIntoView(row)
}

function scrollSelectedRowIntoView(row?: HTMLElement | null) {
  const body = treeBody.value
  if (!body || !row)
    return
  const item = row.querySelector('.el-tree-node__content')?.getBoundingClientRect()
  const viewport = body.getBoundingClientRect()
  if (!item)
    return
  if (item.top < viewport.top)
    body.scrollTop += item.top - viewport.top
  else if (item.bottom > viewport.bottom)
    body.scrollTop += item.bottom - viewport.bottom
}

function isSelectedPath(path: readonly string[]) {
  return path.every((item, index) => item === selectedPath.value[index])
}

watch(selectedPath, (_, previous) => {
  if (!isSelectedPath(previous))
    void revealSelectedPath()
})

const searchResult = computed(() => {
  const keyword = normalizeKeyword(state.filterKey)
  if (!keyword) {
    return {
      tree: state.treeData,
      expandedKeys: state.expandedKeys,
    }
  }
  return filterTree(getSearchTreeSource(), keyword)
})

const displayTreeData = computed(() => searchResult.value.tree)
const displayExpandedKeys = computed(() => state.filterKey
  ? mergeExpandedKeys(searchResult.value.expandedKeys, searchExpansion)
  : state.expandedKeys)

async function refreshTableTree() {
  if (state.refreshing)
    return

  const recoveringInitialLoad = rootStatus.value === 'error' && !state.treeData.length
  state.refreshing = true
  try {
    if (!await loadCatalogs() || disposed)
      return

    refreshRevision += 1
    cancelScheduledSearch()
    branchStatus.clear()
    const expandedCatalogs = state.treeData.filter(node => displayExpandedKeys.value.includes(node.key))
    await Promise.all(expandedCatalogs.map(retryBranch))
    if (recoveringInitialLoad)
      await revealSelectedPath()

    if (state.searchKey.trim()) {
      cancelScheduledSearch()
      await loadSearchTables(state.searchKey, true)
    }
  }
  finally {
    state.refreshing = false
  }
}

async function retryBranch(branch: TreeNode) {
  if (!await loadChildren(branch) || branch.nodeType !== 'catalog'
    || !displayExpandedKeys.value.includes(branch.key)) {
    return
  }
  const expanded = new Set(displayExpandedKeys.value)
  const catalog = findCatalog(branch.catalog)
  const databases = (catalog?.children || []).filter(node =>
    expanded.has(node.key) && (!state.filterKey || loadedRevisions.has(node)),
  )
  await Promise.all(databases.map(loadChildren))
}

function setTreeExpanded(node: TreeNode, expanded: boolean) {
  if (node.nodeType === 'table' || expanded === displayExpandedKeys.value.includes(node.key))
    return

  if (state.filterKey) {
    searchExpansion.set(node.key, expanded)
    if (expanded && !findBranch(node)?.children && !state.searchTablesByDatabase[node.key])
      void loadChildren(node)
  }
  else {
    setBrowseExpanded(node.key, expanded)
    if (expanded)
      void retryBranch(node)
  }
}

function toggleNodeExpand(node: TreeNode) {
  setTreeExpanded(node, !displayExpandedKeys.value.includes(node.key))
}

function syncCurrentTable() {
  const tree = treeRef.value
  const selected = selectedKey.value
  if (tree && (tree.getCurrentKey() !== selected || tree.getCurrentNode() !== tree.getNode(selected || '')?.data))
    tree.setCurrentKey(selected, false)
}

// ElTree's default-expanded-keys expands new entries but does not close removed
// entries. Reconcile the public node handles so search and history stay controlled.
// https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/tree.md
let synchronizedKeys = new Set<string>()
watch([treeRef, displayTreeData, displayExpandedKeys, selectedKey, treeVersion], () => {
  const tree = treeRef.value
  if (!tree)
    return
  const expanded = new Set(displayExpandedKeys.value)
  for (const key of synchronizedKeys) {
    if (!expanded.has(key))
      tree.getNode(key)?.collapse()
  }
  for (const key of expanded) {
    const node = tree.getNode(key)
    if (node && !node.expanded)
      node.expand(undefined, false)
  }
  synchronizedKeys = expanded
  syncCurrentTable()
}, { flush: 'post' })

onBeforeMount(async () => {
  try {
    const stored = JSON.parse(sessionStorage.getItem(expandedKeysSessionKey) || '[]')
    if (Array.isArray(stored))
      state.expandedKeys = getStoredCatalogExpandedKeys(stored.filter(key => typeof key === 'string'))
  }
  catch {
    // Invalid or unavailable browser storage does not block the explorer.
  }

  if (!await loadCatalogs() || disposed)
    return
  const expandedCatalogs = state.treeData.filter(node => state.expandedKeys.includes(node.key))
  await Promise.all([revealSelectedPath(), ...expandedCatalogs.map(loadChildren)])
})
</script>

<template>
  <div class="table-explorer">
    <div class="table-explorer-header">
      <el-input
        v-model="state.searchKey"
        :placeholder="t('tableExplorerSearchPlaceholder')"
        :aria-describedby="showSearchScope ? 'table-explorer-search-scope' : undefined"
        clearable
        class="search-input"
      >
        <template #prefix>
          <Search class="amoro-icon search-input-prefix-icon" />
        </template>
      </el-input>
      <el-tooltip :content="t('tableExplorerRefreshTooltip')">
        <el-button
          text
          size="small"
          :loading="state.refreshing || (rootStatus === 'loading' && !state.treeData.length)"
          :aria-label="t('tableExplorerRefreshTooltip')"
          class="refresh-button"
          @click="refreshTableTree"
        >
          <template #icon>
            <Refresh class="amoro-icon" />
          </template>
        </el-button>
      </el-tooltip>
    </div>

    <p v-if="showSearchScope" id="table-explorer-search-scope" class="search-scope">
      {{ t('tableExplorerSearchScope') }}
    </p>
    <div v-if="rootStatus === 'error'" class="explorer-status" role="status">
      <span>{{ t('tableExplorerLoadCatalogsFailed') }}</span>
      <el-button link type="primary" @click="refreshTableTree">
        {{ t('retry') }}
      </el-button>
    </div>
    <div v-if="searchStatus === 'error'" class="explorer-status" role="status">
      <span>{{ t('tableExplorerSearchFailed') }}</span>
      <el-button link type="primary" @click="loadSearchTables(state.searchKey, true)">
        {{ t('retry') }}
      </el-button>
    </div>
    <div ref="treeBody" class="table-explorer-body" @keydown.capture="handleTreeKeydown">
      <el-tree
        v-if="displayTreeData.length"
        ref="treeRef"
        node-key="key"
        :data="displayTreeData"
        :default-expanded-keys="displayExpandedKeys"
        :current-node-key="selectedKey ?? undefined"
        :props="{ label: 'title', children: 'children' }"
        :expand-on-click-node="false"
        :auto-expand-parent="false"
        highlight-current
        @node-expand="(data: TreeNode) => setTreeExpanded(data, true)"
        @node-collapse="(data: TreeNode) => setTreeExpanded(data, false)"
        @node-click="handleTreeSelect"
      >
        <template #default="{ data: dataRef, node }">
          <div class="tree-node-title" :aria-busy="(dataRef.nodeType !== 'table' && branchStatus.get(dataRef.key) === 'loading') || undefined">
            <span
              class="tree-node-toggle"
              :class="{ 'tree-node-toggle--open': node.expanded }"
              aria-hidden="true"
            >
              <template v-if="dataRef.nodeType !== 'table'">
                <Loading v-if="branchStatus.get(dataRef.key) === 'loading'" class="tree-node-spinner" />
                <CaretRight v-else />
              </template>
            </span>
            <svg class="tree-node-icon" aria-hidden="true">
              <use :href="`${spriteUrl}#icon-${getNodeIcon(dataRef)}`" />
            </svg>
            <span class="tree-node-text" :title="dataRef.title">{{ dataRef.title }}</span>
            <span
              v-if="node.expanded && dataRef.children?.length === 0 && !branchStatus.has(dataRef.key)"
              class="tree-node-empty"
            >{{ t('tableExplorerEmptyBranch') }}</span>
            <el-button
              v-if="dataRef.nodeType !== 'table' && branchStatus.get(dataRef.key) === 'error'"
              link
              type="danger"
              size="small"
              :aria-label="t('tableExplorerRetryBranch', { name: dataRef.title })"
              @click.stop="retryBranch(dataRef)"
            >
              {{ t('retry') }}
            </el-button>
          </div>
        </template>
      </el-tree>
      <div v-else class="explorer-status" role="status">
        <span v-if="rootStatus === 'loading' && !state.treeData.length">{{ t('tableExplorerLoadingCatalogs') }}</span>
        <span v-else-if="searchStatus === 'loading'">{{ t('tableExplorerSearching') }}</span>
        <span v-else-if="state.filterKey && searchStatus !== 'error'">{{ t('tableExplorerNoResults') }}</span>
        <span v-else-if="rootStatus === 'ready' && !state.treeData.length">{{ t('tableExplorerNoCatalogs') }}</span>
      </div>
    </div>
  </div>
</template>

<style lang="less" scoped>
.table-explorer {
  position: relative;
  box-sizing: border-box;
  width: 100%;
  height: 100%;
  padding: 0 0;
  background-color: #fff;
  font-size: 13px;
  display: flex;
  flex-direction: column;

  .table-explorer-header {
    padding: 0 7px 0 8px;
    margin-bottom: 8px;
    display: flex;
    align-items: center;
    gap: 4px;

    .search-input {
      flex: 1;
      min-width: 0;

      :deep(.el-input__wrapper) {
        height: 24px;
      }

      :deep(.el-input__prefix) {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 24px;
      }

      :deep(.search-input-prefix-icon) {
        font-size: 16px;
        line-height: 24px;
      }

      :deep(.el-input__inner) {
        line-height: 24px;
        font-size: 14px;

        &::placeholder {
          font-size: 14px;
          color: #ccc;
        }
      }
    }

    .refresh-button {
      flex: 0 0 auto;
      min-width: 28px;
      min-height: 26px;
    }
  }

  .search-scope,
  .explorer-status {
    margin: 0;
    padding: 4px 8px 8px;
    color: var(--el-text-color-secondary);
  }

  .explorer-status {
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 8px;
  }

  .table-explorer-body {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 0 7px 0 8px;

    :deep(.el-tree),
    :deep(.el-tree-node__content) {
      min-width: 0;
    }

    :deep(.el-tree) {
      background-color: #fff;
    }

    // Use the same row click for expansion, including when a branch has no
    // children loaded and Element Plus considers it a leaf.
    :deep(.el-tree-node__expand-icon) {
      display: none;
    }
  }

  .tree-node-title {
    display: flex;
    align-items: center;
    width: 100%;
    min-width: 0;
    padding-right: 6px;
    gap: 6px;
  }

  .tree-node-toggle {
    flex: 0 0 22px;
    display: grid;
    place-items: center;
    color: var(--el-text-color-secondary);

    svg {
      width: 12px;
      height: 12px;
    }
  }

  .tree-node-toggle--open svg:not(.tree-node-spinner) {
    transform: rotate(90deg);
  }

  .tree-node-spinner {
    color: var(--el-color-primary);
    animation: tree-node-rotate 1s linear infinite;
  }

  .tree-node-icon {
    flex: 0 0 1em;
    height: 1em;
    fill: currentColor;
  }

  .tree-node-text {
    min-width: 0;
    overflow: hidden;
    text-overflow: ellipsis;
    white-space: nowrap;
  }

  @keyframes tree-node-rotate {
    to { transform: rotate(360deg); }
  }

  .tree-node-empty {
    color: var(--el-text-color-secondary);
    font-size: 12px;
  }
}
</style>
