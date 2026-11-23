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
import { Search } from '@element-plus/icons-vue'
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import axios from 'axios'
import TagRowActions from './TagRowActions.vue'
import TagRetentionModal from './TagRetentionModal.vue'
import TagSnapshotSelect from './TagSnapshotSelect.vue'
import { canDeleteTag, canUpdateTagRetention, resolvedTagRetention, sameRetentionSource, tagErrorMessages, validTagForm } from './tag-management-state'
import { requestEpoch } from './snapshot-page-state'
import { useMediaQuery } from '@/hooks/useMediaQuery'
import DashboardPagination from '@/components/DashboardPagination.vue'
import { tableColumnProps } from '@/utils/table'
import { message } from '@/utils/ui-feedback'
import { copyTag, createTag, deleteTag, getTableDetail, getTags } from '@/services/table.service'
import type { TableTag, TagCapabilities, TagScope, TagWriteRequest } from '@/types/tag.type'
import { canManageTable } from '@/utils/permission'
import { tagMutationFailureMessage } from '@/services/tag-mutation'
import { dateFormat } from '@/utils'

const route = useRoute()
const router = useRouter()
const { t } = useI18n()
const source = { catalog: String(route.query.catalog), db: String(route.query.db), table: String(route.query.table) }
const requests = requestEpoch()
const rows = ref<TableTag[]>([])
const capabilities = ref<TagCapabilities | null>(null)
const loading = ref(false)
const busy = ref(false)
// Independent reactive fields keep failed refreshes from erasing a mutation outcome.
// https://vuejs.org/guide/essentials/reactivity-fundamentals.html#reactive
const errors = reactive({ mutation: '', read: '' })
const errorMessages = computed(() => tagErrorMessages(errors))
const keyword = ref(String(route.query.tagKeyword || ''))
const page = ref(Math.max(1, Number(route.query.tagPage) || 1))
const pageSize = ref(20)
const total = ref(0)
const typedSnapshotReferences = ref(false)
const editing = ref(false)
const confirming = ref(false)
const removing = ref<TableTag | null>(null)
const copying = ref<TableTag | null>(null)
const retentionRow = ref<TableTag | null>(null)
const typedName = ref('')
const form = reactive({ name: '', version: '', mode: 'NATIVE_DEFAULT', hours: 24 as number | null })
const scope = computed<TagScope>(() => copying.value?.scope || capabilities.value?.supportedScopes[0] || { kind: 'DEFAULT' })
const compactTags = useMediaQuery('(max-width: 760px)')
const modes = computed(() => capabilities.value?.retention.supportedModes || [])
const allowed = (action: string) => canManageTable() && capabilities.value?.actions.includes(action)
function tagActionState(row: TableTag) {
  return {
    canCopy: allowed('COPY'),
    canAdjust: allowed('UPDATE_RETENTION'),
    canDelete: allowed('DELETE'),
    observed: !busy.value && !loading.value && !!row.observationToken,
    canAdjustRetention: !loading.value && canUpdateTagRetention(row, capabilities.value, busy.value),
  }
}
const valid = computed(() => validTagForm(form.name, form.version, form.mode, form.hours) && !!resolvedTagRetention(form.mode, form.hours, capabilities.value))
const columns = computed(() => [
  { title: t('tagName'), dataIndex: 'name', minWidth: 160, ellipsis: true },
  { title: t('snapshotId'), dataIndex: 'version', minWidth: 112 },
  { title: t('commitTime'), dataIndex: 'committedAt', minWidth: 170 },
  { title: t('tagRetention'), dataIndex: 'retention', minWidth: 136 },
  { title: t('operation'), dataIndex: 'actions', width: 160 },
])
function retentionText(value?: { state: string, ageLimit: string, maxAgeMillis?: number }) {
  if (!value || value.state !== 'KNOWN' || value.ageLimit === 'UNKNOWN')
    return t('tagUnknown')
  return value.ageLimit === 'UNSET' ? t('tagNoAgeLimit') : t('tagHoursValue', { hours: (value.maxAgeMillis || 0) / 3600000 })
}
const defaultText = computed(() => retentionText(capabilities.value?.retention.nativeDefault))
const chosenText = computed(() => form.mode === 'NATIVE_DEFAULT' ? defaultText.value : form.mode === 'NO_AGE_LIMIT' ? t('tagNoAgeLimit') : t('tagHoursValue', { hours: form.hours }))
const ageModel = computed(() => t(capabilities.value?.retention.model === 'TARGET_COMMIT_AGE' ? 'tagCommitAge' : 'tagCreationAge'))
function failureText(failure: unknown, mutation = false) {
  if (mutation)
    return tagMutationFailureMessage(failure, t('tagOutcomeUnknown'))
  if (axios.isAxiosError(failure)) {
    const body = failure.response?.data
    if (typeof body?.message === 'string')
      return body.message
  }
  return failure instanceof Error ? failure.message : t('tagReadFailed')
}
async function load() {
  const ticket = requests.begin()
  loading.value = true
  errors.read = ''
  try {
    const [list, detail] = await Promise.all([getTags({ ...source, page: page.value, pageSize: pageSize.value, keyword: keyword.value }), getTableDetail(source)])
    if (!requests.current(ticket))
      return
    const lastPage = Math.max(1, Math.ceil(list.total / pageSize.value))
    if (page.value > lastPage) {
      page.value = lastPage
      await router.replace({ query: { ...route.query, tagPage: String(lastPage) } })
      await load()
      return
    }
    const previousCapabilities = JSON.stringify(capabilities.value)
    const nextCapabilities: TagCapabilities | null = detail.tagWriteCapabilities ?? null
    const changedSource = (row: TableTag | null) => row && !sameRetentionSource(row, list.list.find(candidate => candidate.name === row.name))
    if ((editing.value && (previousCapabilities !== JSON.stringify(nextCapabilities) || changedSource(copying.value))) || changedSource(removing.value)) {
      editing.value = false
      confirming.value = false
      removing.value = null
      errors.read = t('tagConfirmExpired')
    }
    if (retentionRow.value && (previousCapabilities !== JSON.stringify(nextCapabilities) || changedSource(retentionRow.value))) {
      retentionRow.value = null
      errors.read = t('tagConfirmExpired')
    }
    rows.value = list.list
    total.value = list.total
    capabilities.value = detail.tagWriteCapabilities ?? null
    typedSnapshotReferences.value = detail.typedSnapshotReferences === true
  }
  catch (failure) {
    if (requests.current(ticket))
      errors.read = failureText(failure)
  }
  finally {
    if (requests.current(ticket))
      loading.value = false
  }
}
async function search() {
  page.value = 1
  await router.replace({ query: { ...route.query, tagKeyword: keyword.value, tagPage: '1' } })
  errors.read = ''
  await load()
}
async function changePage(next: number, size: number) {
  page.value = next
  pageSize.value = size
  await router.replace({ query: { ...route.query, tagPage: String(next) } })
  await load()
}
function openForm(row: TableTag | null) {
  copying.value = row ? { ...row } : null
  form.name = row ? `${row.name}_copy` : ''
  form.version = row?.target.id || ''
  form.mode = modes.value.includes('NATIVE_DEFAULT') ? 'NATIVE_DEFAULT' : modes.value[0] || ''
  form.hours = 24
  confirming.value = false
  editing.value = true
}
function openDelete(row: TableTag) {
  removing.value = { ...row }
  typedName.value = ''
}
function body(): TagWriteRequest {
  return {
    scope: scope.value,
    retention: { mode: form.mode, ...(form.mode === 'AGE_LIMIT' ? { maxAgeMillis: Number(form.hours) * 3600000 } : {}) },
    expectedRetentionModel: capabilities.value?.retention.model || '',
    ...(form.mode === 'NATIVE_DEFAULT' ? { expectedRetentionConfigToken: capabilities.value?.retention.configObservationToken } : {}),
  }
}
async function submit() {
  if (busy.value || !valid.value || (copying.value && !confirming.value))
    return
  const expected = resolvedTagRetention(form.mode, form.hours, capabilities.value)
  if (!expected)
    return
  busy.value = true
  errors.read = ''
  errors.mutation = ''
  try {
    const payload = body()
    if (copying.value)
      await copyTag(source, copying.value.name, { ...payload, newName: form.name, expectedSourceToken: copying.value.observationToken }, copying.value.target.id, expected)
    else await createTag(source, { ...payload, name: form.name, target: { id: form.version, branch: scope.value.branch } }, expected)
    editing.value = false
    message.success(t('tagSaved'))
    await load()
  }
  catch (failure) {
    errors.mutation = failureText(failure, true)
    editing.value = false
    await load()
  }
  finally { busy.value = false }
}
async function remove() {
  const row = removing.value
  if (!row || !canDeleteTag(row.name, typedName.value, busy.value))
    return
  busy.value = true
  errors.read = ''
  errors.mutation = ''
  try {
    const result = await deleteTag(source, row)
    removing.value = null
    message.success(t(['BEST_EFFORT_UNVERIFIED', 'UNKNOWN'].includes(result.historyCleanup) ? 'tagDeletedCleanup' : 'tagDeleted'))
    await load()
  }
  catch (failure) {
    errors.mutation = failureText(failure, true)
    removing.value = null
    await load()
  }
  finally { busy.value = false }
}
async function retentionSaved() {
  message.success(t('tagRetentionSaved'))
  retentionRow.value = null
  await load()
}
async function retentionFailed(failure: unknown) {
  errors.mutation = failureText(failure, true)
  retentionRow.value = null
  await load()
}
function openSnapshot(row: TableTag) {
  void router.push({ query: { ...route.query, tab: 'Snapshots', snapshotId: row.target.id, refType: 'TAG', ref: row.name, scopeKind: row.scope.kind, branch: row.scope.branch } })
}
onMounted(load)
onBeforeUnmount(() => requests.invalidate())
</script>

<template>
  <div class="tags-page">
    <el-alert v-for="failure in errorMessages" :key="failure.kind" type="error" :title="failure.message" show-icon class="g-mb-16" :closable="false" />
    <el-alert v-if="capabilities?.unavailableReason" type="info" :title="$t('tagReadOnly')" :description="$t('tagUnavailable')" class="g-mb-16" :closable="false" />
    <div class="tag-toolbar">
      <el-input v-model="keyword" :placeholder="$t('tagSearch')" :maxlength="255" :disabled="busy" class="tag-search" @keydown.enter="(event: KeyboardEvent) => { if (!event.isComposing) search() }">
        <template #append>
          <el-button :icon="Search" :aria-label="$t('tagSearch')" :disabled="busy" @click="search" />
        </template>
      </el-input>
      <el-space class="tag-toolbar-actions">
        <el-button :disabled="busy || loading" @click="load">
          {{ $t('tagRefresh') }}
        </el-button>
        <el-button v-if="allowed('CREATE')" type="primary" :disabled="busy || loading" @click="openForm(null)">
          {{ $t('tagCreate') }}
        </el-button>
      </el-space>
    </div>
    <el-table v-if="!compactTags" v-loading="loading" row-key="name" :data="rows">
      <el-table-column v-for="column in columns" :key="column.dataIndex" v-bind="tableColumnProps(column, columns)">
        <template v-if="column.dataIndex === 'actions' || column.dataIndex === 'committedAt' || column.dataIndex === 'retention' || column.dataIndex === 'version'" #default="{ row: record }">
          <el-button v-if="column.dataIndex === 'version' && typedSnapshotReferences" link type="primary" @click="openSnapshot(record)">
            {{ record.target.id }}
          </el-button>
          <template v-else-if="column.dataIndex === 'version'">
            {{ record.target.id }}
          </template>
          <template v-else-if="column.dataIndex === 'committedAt'">
            {{ record.versionCommittedAtMillis ? dateFormat(record.versionCommittedAtMillis) : '—' }}
          </template>
          <template v-else-if="column.dataIndex === 'retention'">
            {{ retentionText(record.retention) }}
          </template>
          <TagRowActions v-else-if="column.dataIndex === 'actions'" v-bind="tagActionState(record)" @copy="openForm(record)" @retention="retentionRow = { ...record }" @remove="openDelete(record)" />
        </template>
      </el-table-column>
    </el-table>
    <div v-else v-loading="loading" class="tag-card-list">
      <el-empty v-if="!rows.length" :image-size="64" />
      <article v-for="record in rows" :key="record.name" class="tag-card">
        <h3>{{ record.name }}</h3>
        <dl>
          <dt>{{ $t('snapshotId') }}</dt>
          <dd>
            <el-button v-if="typedSnapshotReferences" link type="primary" @click="openSnapshot(record)">
              {{ record.target.id }}
            </el-button>
            <span v-else>{{ record.target.id }}</span>
          </dd>
          <dt>{{ $t('commitTime') }}</dt>
          <dd>{{ record.versionCommittedAtMillis ? dateFormat(record.versionCommittedAtMillis) : '—' }}</dd>
          <dt>{{ $t('tagRetention') }}</dt>
          <dd>{{ retentionText(record.retention) }}</dd>
        </dl>
        <TagRowActions v-bind="tagActionState(record)" @copy="openForm(record)" @retention="retentionRow = { ...record }" @remove="openDelete(record)" />
      </article>
    </div>
    <DashboardPagination
      :pagination="{
        current: page,
        pageSize,
        total,
        showQuickJumper: true,
        showSizeChanger: true,
        pageSizeOptions: [20, 50, 100],
        showTotal: value => $t('tagTotal', { total: value, pages: Math.ceil(value / pageSize) }),
      }"
      :disabled="busy || loading"
      @change="({ current, pageSize: size }) => changePage(current, size)"
    />
    <el-dialog :model-value="editing" :title="copying ? $t(confirming ? 'tagCopyConfirm' : 'tagCopy') : $t('tagCreate')" :show-close="!busy" :close-on-click-modal="false" :close-on-press-escape="!busy" :width="520" @update:model-value="(open: boolean) => { if (!open) editing = false }">
      <p>{{ source.catalog }}.{{ source.db }}.{{ source.table }} · {{ scope.branch || scope.kind }}</p>
      <el-alert v-if="copying" type="info" :title="$t('tagSameSnapshot')" class="g-mb-16" :closable="false" />
      <el-form v-if="!confirming" label-position="top">
        <el-form-item v-if="copying" :label="$t('tagSource')">
          {{ copying.name }} → {{ copying.target.id }}
        </el-form-item>
        <el-form-item :label="$t('tagName')">
          <el-input v-model="form.name" :maxlength="255" :disabled="busy" />
        </el-form-item>
        <el-form-item :label="$t('snapshotId')" for="tag-create-snapshot">
          <el-input v-if="copying" id="tag-create-snapshot" :model-value="form.version" disabled />
          <TagSnapshotSelect v-else v-model:value="form.version" :source="source" :scope="scope" :active="editing" :disabled="busy" />
        </el-form-item>
        <el-form-item :label="$t('tagRetention')">
          <el-select v-model="form.mode" :disabled="busy">
            <el-option v-for="mode in modes" :key="mode" :label="$t(`tagMode${mode}`)" :value="mode" />
          </el-select>
        </el-form-item>
        <el-form-item v-if="form.mode === 'AGE_LIMIT'" :label="$t('tagHours')">
          <el-input-number v-model="form.hours" :min="0.001" :max="2501999792" :disabled="busy" />
        </el-form-item>
      </el-form>
      <el-descriptions v-else :column="1" border>
        <el-descriptions-item :label="$t('tagSource')">
          {{ copying?.name }}
        </el-descriptions-item>
        <el-descriptions-item :label="$t('tagName')">
          {{ form.name }}
        </el-descriptions-item>
        <el-descriptions-item :label="$t('snapshotId')">
          {{ form.version }}
        </el-descriptions-item>
        <el-descriptions-item :label="$t('tagRetention')">
          {{ chosenText }}
        </el-descriptions-item>
      </el-descriptions>
      <p class="g-mt-16">
        {{ ageModel }} · {{ chosenText }}
      </p>
      <p>{{ $t('tagExpiryNotice') }}</p>
      <template #footer>
        <el-button :disabled="busy" @click="editing = false">
          {{ $t('cancel') }}
        </el-button>
        <el-button v-if="confirming" :disabled="busy" @click="confirming = false">
          {{ $t('tagBackEdit') }}
        </el-button>
        <el-button v-if="copying && !confirming" type="primary" :disabled="!valid || busy" @click="confirming = true">
          {{ $t('tagNextConfirm') }}
        </el-button>
        <el-button v-else type="primary" :disabled="!valid || busy" :loading="busy" @click="submit">
          {{ $t(copying ? 'tagConfirmCopy' : 'tagCreate') }}
        </el-button>
      </template>
    </el-dialog>
    <TagRetentionModal :row="retentionRow" :capabilities="capabilities" :source="source" @close="retentionRow = null" @pending="busy = $event" @start="errors.read = ''; errors.mutation = ''" @saved="retentionSaved" @failure="retentionFailed" />
    <el-dialog :model-value="!!removing" :title="$t('tagDeleteConfirm')" :show-close="!busy" :close-on-click-modal="false" :close-on-press-escape="!busy" :width="520" @update:model-value="(open: boolean) => { if (!open) removing = null }">
      <p>{{ source.catalog }}.{{ source.db }}.{{ source.table }} · {{ removing?.scope.branch || removing?.scope.kind }}</p>
      <el-descriptions :column="1" border>
        <el-descriptions-item :label="$t('tagName')">
          {{ removing?.name }}
        </el-descriptions-item>
        <el-descriptions-item :label="$t('snapshotId')">
          {{ removing?.target.id }}
        </el-descriptions-item>
      </el-descriptions>
      <el-alert type="warning" :title="$t(capabilities?.deleteEffect === 'MAY_CLEAN_HISTORY' ? 'tagDeleteHistoryWarning' : 'tagDeleteReferenceWarning')" show-icon class="g-mt-16 g-mb-16" :closable="false" />
      <p>{{ $t('tagTypeName') }}</p>
      <el-input v-model="typedName" :disabled="busy" :placeholder="removing?.name" />
      <template #footer>
        <el-button :disabled="busy" @click="removing = null">
          {{ $t('cancel') }}
        </el-button>
        <el-button type="danger" :loading="busy" :disabled="!removing || !canDeleteTag(removing.name, typedName, busy)" @click="remove">
          {{ $t('tagConfirmDelete') }}
        </el-button>
      </template>
    </el-dialog>
  </div>
</template>

<style scoped>
.tags-page { padding-top: 16px; }
.tag-toolbar { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; }
.tag-search { width: 320px; max-width: 100%; }
.tag-toolbar-actions { margin-left: auto; flex-shrink: 0; }
.tag-card-list { display: grid; gap: 12px; }
.tag-card { padding: 12px; border: 1px solid var(--el-border-color-light); border-radius: 6px; overflow-wrap: anywhere; }
.tag-card h3 { margin: 0 0 8px; font-size: 1rem; }
.tag-card dl { display: grid; grid-template-columns: minmax(0, 0.9fr) minmax(0, 1.1fr); gap: 4px 8px; margin: 0 0 12px; }
.tag-card dt { color: var(--el-text-color-regular); }
.tag-card dd { margin: 0; min-width: 0; }
.tag-card :deep(.el-button.is-link) { padding: 0; max-width: 100%; }
@media (max-width: 760px) {
  .tag-search { width: 100%; }
  .tag-toolbar-actions { margin-left: 0; }
}
</style>
