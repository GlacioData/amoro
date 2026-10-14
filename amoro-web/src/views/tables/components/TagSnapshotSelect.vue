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
-->

<script setup lang="ts">
import { computed, onBeforeUnmount, watch } from 'vue'
import { snapshotPicker } from './tag-snapshot-state'
import { getSnapshots } from '@/services/table.service'
import type { TagScope } from '@/types/tag.type'
import { dateFormat } from '@/utils'

const props = defineProps<{
  source: { catalog: string, db: string, table: string }
  scope: TagScope
  active: boolean
  disabled: boolean
  value: string
}>()
const emit = defineEmits<{ (event: 'update:value', value: string): void }>()
const picker = snapshotPicker((page, pageSize) => getSnapshots({ ...props.source, page, pageSize, ref: props.scope.branch || 'main', operation: 'all' }, false))
const options = computed(() => picker.state.rows.map(row => ({
  value: row.snapshotId,
  label: `${row.snapshotId} · ${row.commitTime && Number.isFinite(Number(row.commitTime)) ? dateFormat(row.commitTime) : '—'}`,
})))
watch(() => [props.active, props.source.catalog, props.source.db, props.source.table, props.scope.kind, props.scope.branch], () => {
  picker.reset()
  if (props.active) {
    emit('update:value', '')
    void picker.load()
  }
}, { immediate: true })
onBeforeUnmount(picker.reset)
function select(value: unknown) {
  emit('update:value', typeof value === 'string' ? value : '')
}
</script>

<template>
  <a-select
    id="tag-create-snapshot"
    :value="value || undefined" :options="options" :loading="picker.state.loading" :disabled="disabled"
    :placeholder="$t('tagChooseSnapshot')" :aria-label="$t('snapshotId')" :not-found-content="picker.state.loading ? $t('loading') : $t('tagNoSnapshots')"
    class="snapshot-select" @change="select"
  />
  <a-button v-if="picker.state.hasMore && !picker.state.error" type="link" :loading="picker.state.loading" :disabled="disabled || picker.state.loading" class="snapshot-more" @click="picker.load">
    {{ $t('tagSnapshotMore') }}
  </a-button>
  <a-alert v-if="picker.state.error" type="error" :message="$t('tagSnapshotReadFailed')" show-icon class="snapshot-error">
    <template #action>
      <a-button size="small" :disabled="disabled || picker.state.loading" @click="picker.load">
        {{ $t('tagSnapshotRetry') }}
      </a-button>
    </template>
  </a-alert>
</template>

<style scoped>
.snapshot-select { width: 100%; }
.snapshot-more { margin-top: 4px; padding-left: 0; }
.snapshot-error { margin-top: 8px; }
</style>
