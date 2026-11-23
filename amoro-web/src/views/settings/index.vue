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

<script lang="ts" setup>
import { computed, reactive, ref, watch } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import { tableColumnProps } from '@/utils/table'
import type { IColumns, IContainerSetting, IKeyAndValue } from '@/types/common.type'
import { getContainersSetting, getSystemSetting } from '@/services/setting.services'
import { usePageScroll } from '@/hooks/usePageScroll'

const { t } = useI18n()
const router = useRouter()
const route = useRoute()
const { pageScrollRef } = usePageScroll()
const loading = ref<boolean>(false)
const systemSettingArray = reactive<IKeyAndValue[]>([])
const containerSetting = reactive<IContainerSetting[]>([])

const basicColumns = computed<IColumns[]>(() => [
  { title: t('key'), dataIndex: 'key', width: 340, ellipsis: true },
  { title: t('value'), dataIndex: 'value' },
])
const activeKey = ref<string[]>([])
const tabMap = {
  system: { title: 'systemSetting', key: 'system' },
  container: { title: 'containerSetting', key: 'container' },
}
const activeKeyTab = ref<string>(tabMap.system.key)
watch(
  () => route,
  (val) => {
    activeKeyTab.value = val.query?.tab as string || tabMap.system.key
    onChangeTab(activeKeyTab.value)
  },
  {
    immediate: true,
  },
)
async function getSystemSettingInfo() {
  try {
    loading.value = true
    const res = await getSystemSetting()
    if (!res)
      return

    systemSettingArray.length = 0
    Object.keys(res).forEach((key) => {
      systemSettingArray.push({
        key,
        value: res[key],
      })
    })
  }
  finally {
    loading.value = false
  }
}
async function getContainersSettingInfo() {
  try {
    loading.value = true
    const res = await getContainersSetting()
    activeKey.value = []
    containerSetting.length = 0;
    (res || []).forEach((ele: any, index: number) => {
      ele.propertiesArray = []
      activeKey.value.push(ele.name)
      containerSetting.push(ele)
      Object.keys(ele.properties || {}).forEach((key) => {
        containerSetting[index].propertiesArray?.push({
          key,
          value: ele.properties[key],
        })
      });
      (ele.optimizeGroup || []).forEach((group: { innerPropertiesArray: { key: string, value: any }[], properties: { [x: string]: any } }) => {
        group.innerPropertiesArray = []
        Object.keys(group.properties || {}).forEach((key) => {
          group.innerPropertiesArray.push({
            key,
            value: group.properties[key],
          })
        })
      })
    })
  }
  finally {
    loading.value = false
  }
}
function onChangeTab(key: string) {
  const query = { ...route.query }
  query.tab = key
  router.replace({ query: { ...query } })
  getSettingInfo()
}
function getSettingInfo() {
  if (activeKeyTab.value === tabMap.system.key) {
    getSystemSettingInfo()
  }
  else {
    getContainersSettingInfo()
  }
}
</script>

<template>
  <div :ref="el => { pageScrollRef = el as HTMLElement | null }" class="page-scroll">
    <div class="setting-wrap">
      <el-tabs v-model="activeKeyTab" @tab-change="(name: string | number) => { const $event = String(name); onChangeTab($event) }">
        <el-tab-pane lazy :key="tabMap.system.key" :name="tabMap.system.key" :label="t(tabMap.system.title)">
          <div class="system-setting">
            <el-table v-if="systemSettingArray.length" row-key="key" :data="systemSettingArray">
              <el-table-column v-for="column in basicColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, basicColumns)" />
            </el-table>
          </div>
        </el-tab-pane>
        <el-tab-pane lazy :key="tabMap.container.key" :name="tabMap.container.key" :label="t(tabMap.container.title)">
          <div class="container-setting">
            <el-collapse v-model="activeKey">
              <el-collapse-item v-for="container in containerSetting" :key="container.name" :name="container.name" :title="container.name">
                <ul class="content">
                  <li class="item">
                    <h3 class="left">
                      {{ $t('name') }}
                    </h3>
                    <span class="right">{{ container.name }}</span>
                  </li>
                  <li v-if="container.classpath" class="item">
                    <h3 class="left">
                      {{ $t('implementation') }}
                    </h3>
                    <span class="right">{{ container.classpath }}</span>
                  </li>
                </ul>
                <h3 class="g-mb-12 g-mt-12">
                  {{ $t('properties') }}
                </h3>
                <el-table row-key="key" :data="container.propertiesArray">
                  <el-table-column v-for="column in basicColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, basicColumns)" />
                </el-table>
                <h3 class="g-mb-12 g-mt-12">
                  {{ $t('optimizerGroups') }}
                </h3>
                <el-collapse>
                  <el-collapse-item v-for="innerGroup in container.optimizeGroup" :key="innerGroup.name" :name="innerGroup.name" :title="innerGroup.name">
                    <el-table row-key="name" :data="innerGroup.innerPropertiesArray">
                      <el-table-column v-for="column in basicColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, basicColumns)" />
                    </el-table>
                  </el-collapse-item>
                </el-collapse>
              </el-collapse-item>
            </el-collapse>
          </div>
        </el-tab-pane>
      </el-tabs>
    </div>
  </div>
  <u-loading v-if="loading" />
</template>

<style lang="less" scoped>
.page-scroll {
  height: 100%;
  overflow-y: auto;
}
 .setting-wrap {
   height: 100%;
   padding: 16px 24px;
  h1,h2,h3 {
    font-weight: 500;
  }
  h1 {
    font-size: 16px;
  }
  h3 {
    font-size: 14px;
  }
  .container-setting {
    :deep(.el-collapse) {
      > .el-collapse-item > .el-collapse-item__header {
        font-weight: 500;
        .el-collapse-item__arrow {
          vertical-align: 1px;
        }
      }
      .el-collapse-item__wrap > .el-collapse-item__content {
        padding: 6px 16px 32px;
      }
      .el-collapse-item:last-child .el-collapse-item__wrap > .el-collapse-item__content {
        padding-bottom: 16px;
      }
    }
  }
  .content {
    .item {
      padding: 6px 0;
      display: flex;
      word-break: break-all;
    }
    .left {
      width: 320px;
      flex-shrink: 0;
      margin-right: 16px;
    }
  }
}
</style>
