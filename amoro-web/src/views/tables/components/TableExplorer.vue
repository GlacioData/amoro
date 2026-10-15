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
import { computed, onBeforeMount, onBeforeUnmount, reactive, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import {
  clearTableCache,
  createTableCache,
  createTableSearchPlan,
  getCachedTables,
  getSelectedPathExpandedKeys,
  getStoredCatalogExpandedKeys,
  getTableCacheGeneration,
  loadTablesWithCache,
  refreshExpandedDatabaseNodes,
} from './table-explorer-state'
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
  isLeaf?: boolean
  children?: TreeNode[]
  // Custom fields
  nodeType: NodeType
  catalog: string
  db?: string
  table?: string
  tableType?: string
}

const router = useRouter()
const route = useRoute()

const storageTableKey = 'easylake-menu-catalog-db-table'
const expandedKeysSessionKey = 'tables_expanded_keys'
const tableCache = createTableCache<TableItem>()
let searchRequestId = 0

const state = reactive({
  loading: false,
  refreshing: false,
  searchKey: '',
  filterKey: '',
  treeData: [] as TreeNode[],
  expandedKeys: [] as string[],
  selectedKeys: [] as string[],
  searchTablesByDatabase: {} as Record<string, TableItem[]>,
  // Cache
  catalogList: [] as string[],
  dbListByCatalog: {} as Record<string, string[]>,
})

function buildCatalogNode(catalog: string): TreeNode {
  return {
    key: `catalog:${catalog}`,
    title: catalog,
    isLeaf: false,
    nodeType: 'catalog',
    catalog,
  }
}

function buildDatabaseNode(catalog: string, db: string): TreeNode {
  return {
    key: `catalog:${catalog}/db:${db}`,
    title: db,
    isLeaf: false,
    nodeType: 'database',
    catalog,
    db,
  }
}

function buildTableNode(catalog: string, db: string, table: TableItem): TreeNode {
  return {
    key: `catalog:${catalog}/db:${db}/table:${table.name}`,
    title: table.name,
    isLeaf: true,
    nodeType: 'table',
    catalog,
    db,
    table: table.name,
    tableType: table.type,
  }
}

function updateTreeNodeChildren(targetKey: string, children: TreeNode[]) {
  const loop = (nodes: TreeNode[]): boolean => {
    for (const node of nodes) {
      if (node.key === targetKey) {
        node.children = children
        return true
      }
      if (node.children && node.children.length && loop(node.children)) {
        return true
      }
    }
    return false
  }

  loop(state.treeData)
}

async function initRootCatalogs() {
  state.loading = true
  try {
    const res = await getCatalogList()
    const catalogs = (res || []).map((item: ICatalogItem) => item.catalogName)
    state.catalogList = catalogs
    state.treeData = catalogs.map(catalog => buildCatalogNode(catalog))
  }
  finally {
    state.loading = false
  }
}

async function loadChildren(node: any) {
  const data = node?.dataRef || node
  if (!data) {
    return
  }

  const nodeType = data.nodeType as NodeType
  if (nodeType === 'catalog') {
    const catalog = data.catalog as string
    if (!catalog || state.dbListByCatalog[catalog]) {
      return
    }

    state.loading = true
    try {
      const res = await getDatabaseList({ catalog, keywords: '' })
      const dbs = (res || []) as string[]
      state.dbListByCatalog[catalog] = dbs
      if (!dbs.length) {
        data.isLeaf = true
        updateTreeNodeChildren(data.key as string, [])
        return
      }
      data.isLeaf = false
      const children = dbs.map(db => buildDatabaseNode(catalog, db))
      updateTreeNodeChildren(data.key as string, children)
    }
    finally {
      state.loading = false
    }
  }
  else if (nodeType === 'database') {
    const catalog = data.catalog as string
    const db = data.db as string
    if (!catalog || !db) {
      return
    }
    state.loading = true
    try {
      const cacheGeneration = getTableCacheGeneration(tableCache)
      const tables = await loadTablesWithCache(
        tableCache,
        catalog,
        db,
        async () => (await getTableList({ catalog, db, keywords: '' }) || []) as TableItem[],
      )
      if (getTableCacheGeneration(tableCache) !== cacheGeneration) {
        return
      }
      if (!tables.length) {
        data.isLeaf = false
        updateTreeNodeChildren(data.key as string, [])
        return
      }
      data.isLeaf = false
      const children = tables.map(table => buildTableNode(catalog, db, table))
      updateTreeNodeChildren(data.key as string, children)
    }
    finally {
      state.loading = false
    }
  }
}

async function expandCatalogBySelected(catalog: string) {
  const safeCatalog = (catalog || '').trim()

  if (!safeCatalog) {
    return
  }

  const catalogKey = `catalog:${safeCatalog}`
  const catalogNode = state.treeData.find(node => node.key === catalogKey)
  if (catalogNode) {
    await loadChildren({ dataRef: catalogNode })
  }

  state.expandedKeys = getSelectedPathExpandedKeys(state.expandedKeys, safeCatalog)
  try {
    sessionStorage.setItem(expandedKeysSessionKey, JSON.stringify(state.expandedKeys))
  }
  catch (e) {
    // ignore sessionStorage write errors
  }
}

function getDatabaseNodes(): TreeNode[] {
  return state.treeData.flatMap(catalogNode =>
    (catalogNode.children || []).filter(node => node.nodeType === 'database'),
  )
}

async function refreshTableTree() {
  if (state.refreshing) {
    return
  }

  state.refreshing = true
  try {
    const databaseNodes = getDatabaseNodes()

    cancelScheduledSearch()
    state.searchTablesByDatabase = {}
    clearTableCache(tableCache)

    await refreshExpandedDatabaseNodes(
      databaseNodes,
      state.expandedKeys,
      (node) => {
        node.children = undefined
        node.isLeaf = false
      },
      node => loadChildren({ dataRef: node }),
    )

    cancelScheduledSearch()
    if (state.searchKey.trim()) {
      await loadSearchTablesSafely(state.searchKey)
    }
  }
  finally {
    state.refreshing = false
  }
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

function handleTreeSelect(selectedKeys: (string | number)[], info: any) {
  const node = info?.node
  if (!node) {
    return
  }

  const dataRef = (node.dataRef || node) as TreeNode | undefined
  if (!dataRef) {
    return
  }

  if (dataRef.nodeType !== 'table') {
    if (!dataRef.isLeaf) {
      toggleNodeExpand(dataRef)
    }
    return
  }

  state.selectedKeys = selectedKeys.map(key => String(key))

  const catalog = (dataRef.catalog || '') as string
  const db = (dataRef.db || '') as string
  const tableName = (dataRef.table || dataRef.title || '') as string
  const tableType = (dataRef.tableType || '') as string

  if (catalog && db && tableName) {
    handleSelectTable(catalog, db, tableName, tableType)
  }
}

async function handleTreeExpand(expandedKeys: (string | number)[], info: any) {
  state.expandedKeys = expandedKeys.map(key => String(key))
  try {
    sessionStorage.setItem(expandedKeysSessionKey, JSON.stringify(state.expandedKeys))
  }
  catch (e) {
    // ignore sessionStorage write errors
  }
  const node = info?.node
  if (!node || node.isLeaf || !info.expanded) {
    return
  }
  await loadChildren(node)
}

async function toggleNodeExpand(dataRef: TreeNode) {
  if (!dataRef || dataRef.isLeaf) {
    return
  }

  const key = String(dataRef.key)
  const hasExpanded = state.expandedKeys.includes(key)

  let nextExpandedKeys: string[]
  if (hasExpanded) {
    nextExpandedKeys = state.expandedKeys.filter(item => item !== key)
  }
  else {
    nextExpandedKeys = [...state.expandedKeys, key]
    await loadChildren({ dataRef })
  }

  state.expandedKeys = nextExpandedKeys
  try {
    sessionStorage.setItem(expandedKeysSessionKey, JSON.stringify(state.expandedKeys))
  }
  catch (e) {
    // ignore sessionStorage write errors
  }
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
        children: state.searchTablesByDatabase[databaseNode.key].map(table =>
          buildTableNode(databaseNode.catalog, databaseNode.db || '', table),
        ),
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

async function loadSearchTables(rawKeyword: string) {
  const requestId = ++searchRequestId
  state.searchTablesByDatabase = {}

  const searchPlan = createTableSearchPlan(rawKeyword, {
    catalog: String(route.query.catalog || ''),
    database: String(route.query.db || ''),
  })
  state.filterKey = searchPlan.filterKeyword

  if (!searchPlan.target) {
    return
  }

  const target = searchPlan.target

  const catalogKey = `catalog:${target.catalog}`
  const catalogNode = state.treeData.find(node => node.key === catalogKey)
  if (!catalogNode) {
    return
  }

  await loadChildren({ dataRef: catalogNode })
  if (requestId !== searchRequestId) {
    return
  }

  const databaseKey = `${catalogKey}/db:${target.database}`
  const databaseNode = catalogNode.children?.find(node => node.key === databaseKey)
  if (!databaseNode) {
    return
  }

  const cachedTables = getCachedTables(tableCache, target.catalog, target.database)
  const tables = cachedTables
    ?? (await getTableList({
      catalog: target.catalog,
      db: target.database,
      keywords: target.keyword,
    }) || []) as TableItem[]

  if (requestId === searchRequestId) {
    state.searchTablesByDatabase[databaseKey] = tables
  }
}

async function loadSearchTablesSafely(rawKeyword: string) {
  try {
    await loadSearchTables(rawKeyword)
  }
  catch (e) {
    // Request errors are reported by the shared request interceptor.
  }
}

function cancelScheduledSearch() {
  if (searchTimer !== undefined) {
    clearTimeout(searchTimer)
    searchTimer = undefined
  }
  searchRequestId += 1
}

watch(
  () => state.searchKey,
  (val) => {
    cancelScheduledSearch()
    searchTimer = setTimeout(() => {
      searchTimer = undefined
      void loadSearchTablesSafely(val || '')
    }, 500)
  },
)

onBeforeUnmount(() => {
  cancelScheduledSearch()
  clearTableCache(tableCache)
})

watch(
  () => route.query,
  async (value, oldValue) => {
    const { catalog, db, table } = value as any
    const { catalog: oldCatalog, db: oldDb, table: oldTable } = (oldValue || {}) as any

    if (`${catalog || ''}${db || ''}${table || ''}` === `${oldCatalog || ''}${oldDb || ''}${oldTable || ''}`) {
      return
    }

    const catalogStr = (catalog as string) || ''
    const dbStr = (db as string) || ''
    const tableStr = (table as string) || ''

    if (catalogStr && dbStr) {
      await expandCatalogBySelected(catalogStr)
      if (tableStr) {
        const tableKey = `catalog:${catalogStr}/db:${dbStr}/table:${tableStr}`
        state.selectedKeys = [tableKey]
      }
      else {
        state.selectedKeys = []
      }
    }
    else {
      state.selectedKeys = []
    }
  },
)

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
const displayExpandedKeys = computed(() => searchResult.value.expandedKeys)

onBeforeMount(async () => {
  await initRootCatalogs()

  let restoredExpandedKeys: string[] = []

  try {
    const stored = sessionStorage.getItem(expandedKeysSessionKey)
    if (stored) {
      const parsed = JSON.parse(stored)
      if (Array.isArray(parsed)) {
        restoredExpandedKeys = getStoredCatalogExpandedKeys(
          parsed.map((key: string | number) => String(key)),
        )
      }
    }
  }
  catch (e) {
    // ignore sessionStorage read/parse errors
  }

  if (restoredExpandedKeys.length) {
    const catalogKeys = restoredExpandedKeys.filter(key => key.startsWith('catalog:') && !key.includes('/db:'))
    for (const catalogKey of catalogKeys) {
      const catalogNode = state.treeData.find(node => node.key === catalogKey)
      if (catalogNode) {
        await loadChildren({ dataRef: catalogNode })
      }
    }

    state.expandedKeys = restoredExpandedKeys
  }

  const query = route.query || {}
  const queryCatalog = (query.catalog as string) || ''
  const queryDb = (query.db as string) || ''
  const queryTable = (query.table as string) || ''

  if (queryCatalog && queryDb) {
    await expandCatalogBySelected(queryCatalog)
    if (queryTable) {
      const tableKey = `catalog:${queryCatalog}/db:${queryDb}/table:${queryTable}`
      state.selectedKeys = [tableKey]
    }
  }
})
</script>

<template>
  <div class="table-explorer">
    <div class="table-explorer-header">
      <a-input
        v-model:value="state.searchKey"
        placeholder="Search catalog.database.table"
        allow-clear
        class="search-input"
      >
        <template #prefix>
          <SearchOutlined class="search-input-prefix-icon" />
        </template>
      </a-input>
      <a-tooltip title="Refresh loaded table lists">
        <a-button
          type="text"
          size="small"
          :loading="state.refreshing"
          aria-label="Refresh loaded table lists"
          class="refresh-button"
          @click="refreshTableTree"
        >
          <template #icon>
            <ReloadOutlined />
          </template>
        </a-button>
      </a-tooltip>
    </div>

    <div class="table-explorer-body">
      <a-tree
        v-if="displayTreeData.length"
        :tree-data="displayTreeData"
        :expanded-keys="displayExpandedKeys"
        :selected-keys="state.selectedKeys"
        :show-line="{ showLeafIcon: false }"
        :show-icon="false"
        :auto-expand-parent="false"
        block-node
        @expand="handleTreeExpand"
        @select="handleTreeSelect"
      >
        <template #title="{ dataRef }">
          <span
            class="tree-node-title"
            :class="`node-${dataRef.nodeType}`"
          >
            <svg-icon
              :icon-class="getNodeIcon(dataRef)"
              class="tree-node-icon"
            />
            <span class="tree-node-text">
              {{ dataRef.title }}
            </span>
          </span>
        </template>
      </a-tree>
      <div v-else class="empty-placeholder">
        <span>No results..</span>
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

      :deep(.ant-input-affix-wrapper) {
        height: 24px;
      }

      :deep(.ant-input-prefix) {
        display: flex;
        align-items: center;
        justify-content: center;
        width: 24px;
      }

      :deep(.search-input-prefix-icon) {
        font-size: 16px;
        line-height: 24px;
      }

      :deep(.ant-input) {
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
    }
  }

  .table-explorer-body {
    flex: 1;
    min-height: 0;
    overflow: auto;
    padding: 0 7px 0 8px;

    :deep(.ant-tree) {
      background-color: #fff;
    }

    :deep(.ant-tree-indent-unit) {
      width: 24px;
    }

    :deep(.ant-tree-switcher-line-icon) {
      color: #d9d9e3;
      font-size: 12px;
    }

    .tree-node-title {
      display: inline-flex;
      align-items: center;

      .tree-node-icon {
        margin-right: 6px;
      }
    }

    .empty-placeholder {
      padding: 8px 4px;
      color: #999;
    }
  }
}

.table-explorer-body .tree-node-title.node-catalog .tree-node-icon,
.table-explorer-body .tree-node-title.node-database .tree-node-icon { transform: translateY(1px); }
.table-explorer-body .tree-node-title.node-table .tree-node-icon { transform: none; }

/* Shift database and table rows left by 2px: indent + switcher + content together */
:deep(.ant-tree-treenode:has(.tree-node-title.node-database)),
:deep(.ant-tree-treenode:has(.tree-node-title.node-table)) {
  transform: translateX(-1px); // Adjust to -3 or -4 for a larger shift if needed
}
</style>
