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
import type { PopoverProps } from 'element-plus'
import { ArrowDown, Check } from '@element-plus/icons-vue'
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { sameSnapshotSelection } from './snapshot-page-state'
import type { IBranchItem, IServiceBranchItem } from '@/types/common.type'
import { branchTypeMap, operationMap } from '@/types/common.type'
import { getBranches, getConsumers, getTags } from '@/services/table.service'

const props = defineProps({ catalog: String, db: String, table: String, disabled: Boolean, bounded: Boolean })
const emit = defineEmits(['refChange', 'consumerChange', 'loadingChange'])

const menuOpen = ref(false)
const pending = ref(false)
const disabled = computed(() => props.disabled || pending.value)
const loadError = ref(false)
const loaded = reactive({ branch: false, tag: false, consumer: false })
let alive = true
onBeforeUnmount(() => {
  alive = false
})

const referencePopperOptions: PopoverProps['popperOptions'] = {
  strategy: 'fixed',
  modifiers: [
    { name: 'flip', options: { rootBoundary: 'viewport', padding: 12 } },
    { name: 'preventOverflow', options: { rootBoundary: 'viewport', padding: 12, altAxis: true, tether: false } },
  ],
}

const selectedObj = ref<IBranchItem>({ value: '', type: branchTypeMap.BRANCH, label: '' })
const branchSearchKey = ref<string>('')
const tagSearchKey = ref<string>('')
const consumerSearchKey = ref<string>('')
const tabActiveKey = ref<string>(branchTypeMap.BRANCH)
const branchList = ref<IBranchItem[]>([])
const tagList = ref<(IBranchItem & { targetId?: string })[]>([])
const tagPage = ref(1)
const tagTotal = ref(0)
const consumerList = ref<IBranchItem[]>([])
const actualBranchList = computed(() => branchList.value.filter(item => !branchSearchKey.value || item.label.includes(branchSearchKey.value)))
const actualTagList = computed(() => tagList.value.filter(item => !tagSearchKey.value || item.label.includes(tagSearchKey.value)))
const actualConsumerList = computed(() => consumerList.value.filter(item => !consumerSearchKey.value
  || item.label.includes(consumerSearchKey.value)))

const operation = ref<string>(operationMap.ALL)
const operationList = reactive([operationMap.ALL, operationMap.OPTIMIZING, operationMap.NONOPTIMIZING])

function onClickInput(e: MouseEvent) {
  e.stopPropagation()
}

function selectObject(obj: IBranchItem) {
  if (disabled.value)
    return
  menuOpen.value = false
  selectedObj.value = obj
  operation.value = operationMap.ALL
  emit('refChange', { ref: obj.value, operation: operationMap.ALL, refType: obj.type === branchTypeMap.TAG ? 'TAG' : 'BRANCH', targetId: (obj as IBranchItem & { targetId?: string }).targetId })
}
function selectConsumer(obj: IBranchItem) {
  if (disabled.value || !obj.amoroCurrentSnapshotsOfTable)
    return
  menuOpen.value = false
  if (sameSnapshotSelection(obj, selectedObj.value))
    return
  selectedObj.value = obj
  const amoroCurrentSnapshotsItem = { ...obj.amoroCurrentSnapshotsOfTable }
  emit('consumerChange', { ref: obj.value, amoroCurrentSnapshotsItem, operation: operationMap.ALL })
}
function onChange(val: string) {
  emit('refChange', { ref: selectedObj.value.value, operation: val, refType: selectedObj.value.type === branchTypeMap.TAG ? 'TAG' : 'BRANCH', targetId: (selectedObj.value as IBranchItem & { targetId?: string }).targetId })
}

async function getBranchList() {
  const result = await getBranches(props as any)
  if (!alive)
    return
  loaded.branch = true
  branchList.value = (result.list || []).map((l: IServiceBranchItem) => ({ value: l.name, label: l.name, type: branchTypeMap.BRANCH }))
  if (!props.bounded)
    branchList.value.length && selectObject(branchList.value[0])
}

async function getTagList() {
  const result = await getTags({ catalog: props.catalog || '', db: props.db || '', table: props.table || '', page: tagPage.value, pageSize: 20, keyword: tagSearchKey.value })
  if (!alive)
    return
  loaded.tag = true
  tagTotal.value = result.total
  tagList.value = (result.list || []).map(l => ({ value: l.name, label: l.name, type: branchTypeMap.TAG, targetId: l.target.id }))
}
async function reloadTags(page = 1) {
  if (pending.value)
    return
  tagPage.value = page
  pending.value = true
  emit('loadingChange', true)
  try { await getTagList() }
  catch {
    if (alive)
      loadError.value = true
  }
  finally { if (alive) { pending.value = false; emit('loadingChange', false) } }
}
async function getConsumerList() {
  const result = await getConsumers(props as any)
  if (!alive)
    return
  loaded.consumer = true
  consumerList.value = (result.list || []).map((l: IServiceBranchItem) => ({ value: l.consumerId, label: l.consumerId, type: branchTypeMap.CONSUMER, amoroCurrentSnapshotsOfTable: l.amoroCurrentSnapshotsOfTable }))
}
async function init() {
  if (props.bounded) {
    await loadTab(branchTypeMap.BRANCH)
  }
  else {
    try {
      await Promise.all([getBranchList(), getTagList(), getConsumerList()])
    }
    catch {
      if (alive)
        loadError.value = true
    }
  }
}

async function loadTab(kind: string) {
  if (!props.bounded || disabled.value)
    return
  const key = kind === branchTypeMap.BRANCH ? 'branch' : kind === branchTypeMap.TAG ? 'tag' : 'consumer'
  if (loaded[key])
    return
  pending.value = true
  loadError.value = false
  emit('loadingChange', true)
  let success = false
  try {
    if (key === 'branch')
      await getBranchList()
    else if (key === 'tag')
      await getTagList()
    else await getConsumerList()
    success = true
  }
  catch {
    if (alive)
      loadError.value = true
  }
  finally {
    if (alive) {
      pending.value = false
      emit('loadingChange', false)
    }
  }
  // Emit the first list request only after selector loading releases its admission state.
  if (alive && success && key === 'branch' && branchList.value.length)
    selectObject(branchList.value[0])
}

onMounted(() => {
  tabActiveKey.value = branchTypeMap.BRANCH
  init()
})
</script>

<template>
  <div class="branch-selector">
    <el-popover v-model:visible="menuOpen" trigger="click" placement="bottom-start" :popper-options="referencePopperOptions" width="min(360px, calc(100vw - 24px))" popper-class="branch-selector-popup" :disabled="disabled">
      <template #reference>
        <el-button class="branch-btn" :disabled="(!selectedObj.value && !props.bounded) || disabled">
          <svg-icon class-name="branch-selector-icon" :icon-class="selectedObj.type" class="g-mr-8" />
          <span class="branch-btn-label">{{ selectedObj.label || $t('branches') }}</span>
          <ArrowDown class="amoro-icon" />
        </el-button>
      </template>
      <div @click.stop>
        <div class="branch-selector-search">
          <el-input v-show="tabActiveKey === branchTypeMap.BRANCH" v-model="branchSearchKey" :placeholder="$t('filterBranchesOrTagsOrConsumers')" @click="onClickInput" />
          <el-input v-show="tabActiveKey === branchTypeMap.TAG" v-model="tagSearchKey" :disabled="pending" :placeholder="$t('filterBranchesOrTagsOrConsumers')" @keydown.enter="reloadTags(1)" @click="onClickInput" />
          <el-input
            v-show="tabActiveKey === branchTypeMap.CONSUMER"
            v-model="consumerSearchKey"
            :placeholder="$t('filterBranchesOrTagsOrConsumers')"
            @click="onClickInput"
          />
        </div>
        <el-alert v-if="loadError" type="error" :title="$t('snapshotReadFailed')" :closable="false" />
        <el-button v-if="loadError" :disabled="disabled" @click="loadTab(tabActiveKey)">
          {{ $t('snapshotRetry') }}
        </el-button>
        <div v-loading="pending">
          <el-tabs v-model="tabActiveKey" type="card" @tab-change="(name: string | number) => { const $event = String(name); loadTab(String($event)) }">
            <el-tab-pane lazy :key="branchTypeMap.BRANCH" :name="branchTypeMap.BRANCH" :label="$t('branches')" :disabled="pending">
              <template v-if="!!actualBranchList.length">
                <button v-for="(item, key) in actualBranchList" :key="key" type="button" class="branch-selector-item" @click="selectObject(item)">
                  <div class="item-icon">
                    <Check v-if="sameSnapshotSelection(item, selectedObj)" class="amoro-icon" />
                  </div>
                  <span class="item-label">{{ item.label }}</span>
                </button>
              </template>
              <span v-else class="empty-tips">{{ $t('nothingToShow') }}</span>
            </el-tab-pane>
            <el-tab-pane lazy :key="branchTypeMap.TAG" :name="branchTypeMap.TAG" :label="$t('tags')" :disabled="pending">
              <template v-if="!!actualTagList.length">
                <button v-for="(item, key) in actualTagList" :key="key" type="button" class="branch-selector-item" @click="selectObject(item)">
                  <div class="item-icon">
                    <Check v-if="sameSnapshotSelection(item, selectedObj)" class="amoro-icon" />
                  </div>
                  <span class="item-label">{{ item.label }}</span>
                </button>
              </template>
              <span v-else class="empty-tips">{{ $t('nothingToShow') }}</span>
              <el-pagination :current-page="tagPage" :total="tagTotal" :page-size="20" :disabled="pending" size="small" layout="prev, pager, next, jumper" @update:current-page="reloadTags" />
            </el-tab-pane>
            <el-tab-pane lazy v-if="props.bounded || consumerList.length !== 0" :key="branchTypeMap.CONSUMER" :name="branchTypeMap.CONSUMER" :label="$t('consumers')" :disabled="pending">
              <template v-if="!!actualConsumerList.length">
                <div v-for="(item, key) in actualConsumerList" :key="key" class="branch-selector-item" role="button" :tabindex="item.amoroCurrentSnapshotsOfTable ? 0 : -1" :aria-disabled="!item.amoroCurrentSnapshotsOfTable" @click="selectConsumer(item)" @keydown.enter.prevent="selectConsumer(item)" @keydown.space.prevent="selectConsumer(item)">
                  <div class="item-icon">
                    <Check v-if="sameSnapshotSelection(item, selectedObj)" class="amoro-icon" />
                  </div>
                  <span class="item-label">{{ item.label }}</span>
                  <span v-if="!item.amoroCurrentSnapshotsOfTable">{{ $t('consumerSnapshotUnavailable') }}</span>
                </div>
              </template>
              <span v-else class="empty-tips">{{ $t('nothingToShow') }}</span>
            </el-tab-pane>
          </el-tabs>
        </div>
      </div>
    </el-popover>
    <div>
      <svg-icon class-name="branch-selector-icon" icon-class="branch" class="g-mr-4" />
      <span class="g-mr-4">{{ props.bounded && !loaded.branch ? '—' : branchList.length }}</span>
      <span>{{ $t('branches') }}</span>
    </div>
    <div>
      <svg-icon class-name="branch-selector-icon" icon-class="tag" class="g-mr-4" />
      <span class="g-mr-4">{{ props.bounded && !loaded.tag ? '—' : tagTotal }}</span>
      <span>{{ $t('tags') }}</span>
    </div>
    <div class="operation-filter">
      {{ $t('operation') }}:
      <el-select
        v-model="operation"
        class="operation-select"
        :disabled="disabled || selectedObj.type === branchTypeMap.CONSUMER || !selectedObj.value"
        @change="onChange"
      >
        <el-option v-for="item in operationList" :key="item" :label="item" :value="item" />
      </el-select>
    </div>
    <div class="selector-extra">
      <slot name="extra" />
    </div>
  </div>
</template>

<style lang="less">
.branch-selector {
  display: flex;
  align-items: center;
  flex-wrap: wrap;
  gap: 8px 16px;

  .branch-btn {
    display: flex;
    align-items: center;
    color: #102048;

    .branch-btn-label {
      line-height: 32px;
      max-width: 125px;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }
  }

  .operation-filter {
    display: flex;
    align-items: center;
    gap: 8px;
    max-width: 100%;
  }

  .operation-select {
    width: 160px;
    min-width: 0;
  }

  .selector-extra { display: flex; align-items: center; }
}

.branch-selector-popup {
    background: #fff;
    height: auto;
    max-height: min(480px, calc(100dvh - 32px));
    overflow: auto;
    padding: 8px 0 16px 0;
    font-size: 12px;
    border-radius: 6px;
    box-shadow: 0 6px 16px 0 rgba(0, 0, 0, 0.08),0 3px 6px -4px rgba(0, 0, 0, 0.12), 0 9px 28px 8px rgba(0, 0, 0, 0.05);

    .branch-selector-search {
      margin: 0 12px 8px 12px;
    }

    .el-tabs {
      margin: 0 12px;

      .el-tab-pane {
        height: min(240px, 40dvh);
        overflow-y: auto;
      }

      .branch-selector-item {
        display: flex;
        align-items: center;
        width: 100%;
        padding: 7px 16px;
        overflow: hidden;
        text-align: left;
        border: 0;
        background: transparent;
        font: inherit;
        cursor: pointer;

        &:hover {
          background-color: rgba(0, 0, 0, 0.04);
        }

        .item-icon {
          margin-right: 4px;
          width: 16px;
        }

        .item-label {
          flex: 1;
          overflow: hidden;
          text-overflow: ellipsis;
          white-space: nowrap;
          color: #102048;
        }
      }
    }
  }

  .empty-tips {
    margin-top: 12px;
    display: block;
    width: 100%;
    text-align: center;
    color: #102048;
  }
</style>
