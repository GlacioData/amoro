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
import { computed, onBeforeUnmount, onMounted, reactive, ref } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { useI18n } from 'vue-i18n'
import axios from 'axios'
import { message } from 'ant-design-vue'
import { ClockCircleOutlined, CopyOutlined, DeleteOutlined } from '@ant-design/icons-vue'
import TagRetentionModal from './TagRetentionModal.vue'
import TagSnapshotSelect from './TagSnapshotSelect.vue'
import { canDeleteTag, canUpdateTagRetention, resolvedTagRetention, sameRetentionSource, tagErrorMessages, validTagForm } from './tag-management-state'
import { requestEpoch } from './snapshot-page-state'
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
const modes = computed(() => capabilities.value?.retention.supportedModes || [])
const allowed = (action: string) => canManageTable() && capabilities.value?.actions.includes(action)
const valid = computed(() => validTagForm(form.name, form.version, form.mode, form.hours) && !!resolvedTagRetention(form.mode, form.hours, capabilities.value))
const columns = computed(() => [
  { title: t('tagName'), dataIndex: 'name' },
  { title: t('snapshotId'), dataIndex: 'version' },
  { title: t('commitTime'), dataIndex: 'committedAt' },
  { title: t('tagRetention'), dataIndex: 'retention' },
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
// Modal open/footer/confirmLoading follow Ant Design Vue 4.2.6:
// https://github.com/vueComponent/ant-design-vue/blob/4.2.6/components/modal/Modal.tsx
</script>

<template>
  <div class="tags-page">
    <a-alert v-for="failure in errorMessages" :key="failure.kind" type="error" :message="failure.message" show-icon class="g-mb-16" />
    <a-alert v-if="capabilities?.unavailableReason" type="info" :message="$t('tagReadOnly')" :description="$t('tagUnavailable')" class="g-mb-16" />
    <div class="tag-toolbar">
      <a-input-search v-model:value="keyword" :placeholder="$t('tagSearch')" :maxlength="255" :disabled="busy" class="tag-search" @search="search" />
      <a-space class="tag-toolbar-actions">
        <a-button :disabled="busy || loading" @click="load">
          {{ $t('tagRefresh') }}
        </a-button>
        <a-button v-if="allowed('CREATE')" type="primary" :disabled="busy || loading" @click="openForm(null)">
          {{ $t('tagCreate') }}
        </a-button>
      </a-space>
    </div>
    <a-table row-key="name" :columns="columns" :data-source="rows" :loading="loading" :pagination="false">
      <template #bodyCell="{ column, record }">
        <a-button v-if="column.dataIndex === 'version' && typedSnapshotReferences" type="link" @click="openSnapshot(record)">
          {{ record.target.id }}
        </a-button>
        <template v-else-if="column.dataIndex === 'version'">
          {{ record.target.id }}
        </template>
        <template v-else-if="column.dataIndex === 'committedAt'">
          {{ record.versionCommittedAtMillis ? dateFormat(record.versionCommittedAtMillis) : '—' }}
        </template>
        <template v-else-if="column.dataIndex === 'retention'">
          {{ retentionText(record.retention) }}
        </template>
        <a-space v-else-if="column.dataIndex === 'actions'">
          <a-tooltip v-if="allowed('COPY')" :title="$t('tagCopy')" :trigger="['hover', 'focus']">
            <a-button shape="circle" :aria-label="$t('tagCopy')" :disabled="busy || loading || !record.observationToken" @click="openForm(record)">
              <CopyOutlined />
            </a-button>
          </a-tooltip>
          <a-tooltip v-if="allowed('UPDATE_RETENTION')" :title="$t('tagUpdateRetention')" :trigger="['hover', 'focus']">
            <a-button shape="circle" :aria-label="$t('tagUpdateRetention')" :disabled="loading || !canUpdateTagRetention(record, capabilities, busy)" @click="retentionRow = { ...record }">
              <ClockCircleOutlined />
            </a-button>
          </a-tooltip>
          <a-tooltip v-if="allowed('DELETE')" :title="$t('tagDelete')" :trigger="['hover', 'focus']">
            <a-button shape="circle" danger :aria-label="$t('tagDelete')" :disabled="busy || loading || !record.observationToken" @click="openDelete(record)">
              <DeleteOutlined />
            </a-button>
          </a-tooltip>
        </a-space>
      </template>
    </a-table>
    <a-pagination :current="page" :page-size="pageSize" :total="total" show-quick-jumper show-size-changer :page-size-options="['20', '50', '100']" :show-total="(value: number) => $t('tagTotal', { total: value, pages: Math.ceil(value / pageSize) })" :disabled="busy || loading" class="g-mt-16" @change="changePage" />
    <a-modal :open="editing" :title="copying ? $t(confirming ? 'tagCopyConfirm' : 'tagCopy') : $t('tagCreate')" :closable="!busy" :mask-closable="false" :keyboard="!busy" @cancel="editing = false">
      <p>{{ source.catalog }}.{{ source.db }}.{{ source.table }} · {{ scope.branch || scope.kind }}</p>
      <a-alert v-if="copying" type="info" :message="$t('tagSameSnapshot')" class="g-mb-16" />
      <a-form v-if="!confirming" layout="vertical">
        <a-form-item v-if="copying" :label="$t('tagSource')">
          {{ copying.name }} → {{ copying.target.id }}
        </a-form-item>
        <a-form-item :label="$t('tagName')">
          <a-input v-model:value="form.name" :maxlength="255" :disabled="busy" />
        </a-form-item>
        <a-form-item :label="$t('snapshotId')" html-for="tag-create-snapshot">
          <a-input v-if="copying" id="tag-create-snapshot" :value="form.version" disabled />
          <TagSnapshotSelect v-else v-model:value="form.version" :source="source" :scope="scope" :active="editing" :disabled="busy" />
        </a-form-item>
        <a-form-item :label="$t('tagRetention')">
          <a-select v-model:value="form.mode" :disabled="busy">
            <a-select-option v-for="mode in modes" :key="mode" :value="mode">
              {{ $t(`tagMode${mode}`) }}
            </a-select-option>
          </a-select>
        </a-form-item>
        <a-form-item v-if="form.mode === 'AGE_LIMIT'" :label="$t('tagHours')">
          <a-input-number v-model:value="form.hours" :min="0.001" :max="2501999792" :disabled="busy" />
        </a-form-item>
      </a-form>
      <a-descriptions v-else :column="1" bordered>
        <a-descriptions-item :label="$t('tagSource')">
          {{ copying?.name }}
        </a-descriptions-item>
        <a-descriptions-item :label="$t('tagName')">
          {{ form.name }}
        </a-descriptions-item>
        <a-descriptions-item :label="$t('snapshotId')">
          {{ form.version }}
        </a-descriptions-item>
        <a-descriptions-item :label="$t('tagRetention')">
          {{ chosenText }}
        </a-descriptions-item>
      </a-descriptions>
      <p class="g-mt-16">
        {{ ageModel }} · {{ chosenText }}
      </p>
      <p>{{ $t('tagExpiryNotice') }}</p>
      <template #footer>
        <a-button :disabled="busy" @click="editing = false">
          {{ $t('cancel') }}
        </a-button>
        <a-button v-if="confirming" :disabled="busy" @click="confirming = false">
          {{ $t('tagBackEdit') }}
        </a-button>
        <a-button v-if="copying && !confirming" type="primary" :disabled="!valid || busy" @click="confirming = true">
          {{ $t('tagNextConfirm') }}
        </a-button>
        <a-button v-else type="primary" :disabled="!valid || busy" :loading="busy" @click="submit">
          {{ $t(copying ? 'tagConfirmCopy' : 'tagCreate') }}
        </a-button>
      </template>
    </a-modal>
    <TagRetentionModal :row="retentionRow" :capabilities="capabilities" :source="source" @close="retentionRow = null" @pending="busy = $event" @start="errors.read = ''; errors.mutation = ''" @saved="retentionSaved" @failure="retentionFailed" />
    <a-modal :open="!!removing" :title="$t('tagDeleteConfirm')" :closable="!busy" :mask-closable="false" :keyboard="!busy" @cancel="removing = null">
      <p>{{ source.catalog }}.{{ source.db }}.{{ source.table }} · {{ removing?.scope.branch || removing?.scope.kind }}</p>
      <a-descriptions :column="1" bordered>
        <a-descriptions-item :label="$t('tagName')">
          {{ removing?.name }}
        </a-descriptions-item>
        <a-descriptions-item :label="$t('snapshotId')">
          {{ removing?.target.id }}
        </a-descriptions-item>
      </a-descriptions>
      <a-alert type="warning" :message="$t(capabilities?.deleteEffect === 'MAY_CLEAN_HISTORY' ? 'tagDeleteHistoryWarning' : 'tagDeleteReferenceWarning')" show-icon class="g-mt-16 g-mb-16" />
      <p>{{ $t('tagTypeName') }}</p>
      <a-input v-model:value="typedName" :disabled="busy" :placeholder="removing?.name" />
      <template #footer>
        <a-button :disabled="busy" @click="removing = null">
          {{ $t('cancel') }}
        </a-button>
        <a-button type="primary" danger :loading="busy" :disabled="!removing || !canDeleteTag(removing.name, typedName, busy)" @click="remove">
          {{ $t('tagConfirmDelete') }}
        </a-button>
      </template>
    </a-modal>
  </div>
</template>

<style scoped>
.tags-page { padding-top: 16px; }
.tag-toolbar { display: flex; align-items: center; flex-wrap: wrap; gap: 12px; margin-bottom: 16px; }
.tag-search { width: 320px; max-width: 100%; }
.tag-toolbar-actions { margin-left: auto; flex-shrink: 0; }
</style>
