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

<script lang="ts">
import { Expand, Fold } from '@element-plus/icons-vue'
import { computed, defineComponent, nextTick, reactive, ref, toRefs, watchEffect } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import useStore from '@/store/index'
import { getQueryString } from '@/utils'
import { useMediaQuery } from '@/hooks/useMediaQuery'
import {
  canManagePlatform,
  canViewCatalog,
  canViewOptimizer,
  canViewSystem,
  canViewTable,
  getDefaultRoute,
} from '@/utils/permission'

interface MenuItem {
  key: string
  title: string
  icon: string
}

export default defineComponent({
  name: 'Sidebar',
  components: { Expand, Fold },
  setup() {
    const { t } = useI18n()
    const router = useRouter()
    const route = useRoute()
    const store = useStore()

    const state = reactive({
      selectedKeys: [] as string[],
    })
    const preferredCollapsed = ref(false)
    const compact = useMediaQuery('(max-width: 1024px)')
    const collapsed = computed(() => compact.value || preferredCollapsed.value)
    const hasToken = computed(() => {
      return !!(getQueryString('token') || '')
    })
    const canAccessSystem = computed(() => canViewSystem())
    const canAccessCatalog = computed(() => canViewCatalog())
    const canAccessTable = computed(() => canViewTable())
    const canAccessOptimizer = computed(() => canViewOptimizer())
    const canAccessSettings = computed(() => canManagePlatform())
    const menuList = computed(() => {
      const menu: MenuItem[] = [
        {
          key: 'tables',
          title: t('tables'),
          icon: 'TableOutlined',
        },
      ]
      const allMenu: MenuItem[] = [
        {
          key: 'overview',
          title: t('overview'),
          icon: 'overview',
        },
        {
          key: 'tables',
          title: t('tables'),
          icon: 'tables',
        },
        {
          key: 'catalogs',
          title: t('catalogs'),
          icon: 'catalogs',
        },
        {
          key: 'optimizing',
          title: t('optimizing'),
          icon: 'optimizers',
        },
        {
          key: 'settings',
          title: t('settings'),
          icon: 'settings',
        },
      ]
      const source = hasToken.value ? menu : allMenu
      return source.filter((item) => {
        if (!canAccessSettings.value && item.key === 'settings') {
          return false
        }
        if (!canAccessSystem.value && item.key === 'overview') {
          return false
        }
        if (!canAccessCatalog.value && item.key === 'catalogs') {
          return false
        }
        if (!canAccessOptimizer.value && item.key === 'optimizing') {
          return false
        }
        if (!canAccessTable.value && item.key === 'tables') {
          return false
        }
        return true
      })
    })

    const setCurMenu = () => {
      const pathArr = route.path.split('/')
      if (route.path) {
        const routePath = [pathArr[1]]
        state.selectedKeys = routePath.includes('hive-tables') ? ['tables'] : routePath
      }
    }
    watchEffect(() => {
      setCurMenu()
    })
    const toggleCollapsed = () => {
      preferredCollapsed.value = !preferredCollapsed.value
      // The stretch animation is 300 ms and the trigger method needs to be delayed here
      setTimeout(() => {
        window.dispatchEvent(new Event('resize'))
      }, 300)
    }

    const navClick = (item: MenuItem) => {
      if (item.key === 'tables') {
        let catalog: string | undefined
        let db: string | undefined
        let tableName: string | undefined
        let type: string | undefined

        try {
          const stored = localStorage.getItem('easylake-menu-catalog-db-table')
          if (stored) {
            const parsed = JSON.parse(stored) as {
              catalog?: string
              database?: string
              tableName?: string
              type?: string
            }
            catalog = parsed.catalog
            db = parsed.database
            tableName = parsed.tableName
            type = parsed.type
          }
        }
        catch (e) {
          // ignore localStorage read/parse errors
        }

        if (catalog && db && tableName && type) {
          router.replace({
            path: '/tables',
            query: {
              catalog,
              db,
              table: tableName,
              type,
            },
          })
        }
        else {
          router.replace({
            path: '/tables',
          })
        }

        nextTick(() => {
          setCurMenu()
        })
        return
      }

      const targetPath = `/${item.key}`
      router.replace({
        path: targetPath,
      })
      nextTick(() => {
        setCurMenu()
      })
    }

    const viewOverview = () => {
      router.push({
        path: canAccessSystem.value ? '/overview' : getDefaultRoute(),
      })
    }

    return {
      ...toRefs(state),
      collapsed,
      compact,
      hasToken,
      canAccessSystem,
      canAccessCatalog,
      canAccessTable,
      canAccessOptimizer,
      canAccessSettings,
      menuList,
      toggleCollapsed,
      navClick,
      store,
      viewOverview,
    }
  },
})
</script>

<template>
  <div :class="{ 'side-bar-collapsed': collapsed }" class="side-bar">
    <div :class="{ 'logo-collapsed': collapsed }" class="logo g-flex-ae" @click="viewOverview">
      <img src="../assets/images/logo1.svg" class="logo-img" alt="">
      <img v-show="!collapsed" src="../assets/images/arctic-dashboard1.svg" class="arctic-name" alt="">
    </div>
    <el-menu
      :default-active="String(selectedKeys[0] ?? '')"
      :collapse="collapsed"
      :collapse-transition="false"
    >
      <el-menu-item v-for="item in menuList" :key="item.key" :index="item.key" :aria-label="item.title" :class="{ 'active-color': (store.isShowTablesMenu && item.key === 'tables'), 'table-item-tab': item.key === 'tables' }" @click="navClick(item)">
        <svg-icon :icon-class="item.icon" class="svg-icon" />
        <template #title>
          <span class="menu-title">{{ item.title }}</span>
        </template>
      </el-menu-item>
    </el-menu>
    <el-button v-if="!compact" link type="primary" :aria-label="$t(collapsed ? 'expandNavigation' : 'collapseNavigation')" :aria-expanded="!collapsed" class="toggle-btn" @click="toggleCollapsed">
      <Expand v-if="collapsed" class="amoro-icon" />
      <Fold v-else class="amoro-icon" />
    </el-button>
  </div>
</template>

<style lang="less" scoped>
  .side-bar {
    position: relative;
    height: 100%;
    transition: width 0.3s;
    display: flex;
    flex-direction: column;
    flex-shrink: 0;
    :deep(.el-menu) {
      height: 100%;
      width: 200px;
      border-right: 0;
      --el-menu-bg-color: #001529;
      --el-menu-text-color: rgba(255, 255, 255, 0.75);
      --el-menu-active-color: #fff;
      --el-menu-hover-bg-color: @dark-bg-color;
      --el-menu-item-height: 40px;
      &.el-menu--collapse {
        width: 64px;
        .el-menu-item {
          width: 100%;
        }
        .logo {
          padding-left: 14px;
        }
        .toggle-btn {
          position: absolute;
          right: -68px;
          top: 8px;
          font-size: 18px;
          padding: 0 24px;
        }
      }
    }
    :deep(.el-menu-item) {
      width: calc(100% - 8px);
      margin: 0;
      border-radius: 8px;
      padding-left: 22px !important;
      .menu-title {
        width: 100%;
        margin-left: 12px;
      }
      &.is-active {
        background-color: @primary-color;
        color: #fff;
      }
      &.active-color {
        color: #fff;
        background-color: @dark-bg-color;
      }
      &:hover {
        color: #fff;
      }
      &.table-item-tab:hover {
        background-color: @dark-bg-color;
      }
    }
    .logo {
      padding: 12px 0 12px 16px;
      overflow: hidden;
      background-color: #001529;
      cursor: pointer;
      padding: 12px 20px;
    }
    .logo-img {
      width: 24px;
      height: 24px;
    }
    .arctic-name {
      width: 66px;
      margin: 4px 0 0 4px;
    }
    .toggle-btn {
      position: absolute;
      z-index: 1;
      right: -68px;
      top: 8px;
      font-size: 18px;
      padding: 0 24px;
    }
    .svg-icon {
      font-size: 16px;
    }
  }
  @media (max-width: 760px) {
    .side-bar :deep(.el-menu.el-menu--collapse) { width: 52px; --el-menu-item-height: 44px; }
    .side-bar .logo { padding-inline: 14px; }
    .side-bar :deep(.el-menu--collapse .el-menu-item) { padding-left: 16px !important; }
  }
  .tables-menu-wrap {
    position: absolute;
    top: 0;
    left: 200px;
    right: 0;
    bottom: 0;
    z-index: 1100;
    &.collapsed-sub-menu {
      left: 64px;
    }
  }
</style>
