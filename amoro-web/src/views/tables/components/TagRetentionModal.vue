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
import { computed, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import type { TableTag, TagCapabilities, TagRetention, TagRetentionUpdateRequest } from '@/types/tag.type'
import { updateTagRetention } from '@/services/table.service'
import { canUpdateTagRetention, resolvedTagRetention } from './tag-management-state'

const props = defineProps<{ row: TableTag | null, capabilities: TagCapabilities | null, source: { catalog: string, db: string, table: string } }>()
const emit = defineEmits<{ close: [], pending: [value: boolean], start: [], saved: [], failure: [value: unknown] }>()
const { t } = useI18n()
const busy = ref(false)
const form = reactive({ mode: '', hours: null as number | null })
const confirmation = ref<{ row: TableTag, body: TagRetentionUpdateRequest, expected: TagRetention } | null>(null)
const modes = computed(() => props.capabilities?.retention.supportedModes || [])
const expected = computed(() => resolvedTagRetention(form.mode, form.hours, props.capabilities))
const valid = computed(() => !!props.row && canUpdateTagRetention(props.row, props.capabilities, busy.value) && !!expected.value)
watch(() => props.row, () => { if (!busy.value) { form.mode = ''; form.hours = null; confirmation.value = null } })
function policyText(value?: TagRetention) {
  if (!value || value.state !== 'KNOWN') return t('tagUnknown')
  if (value.ageLimit === 'UNSET') return t('tagNoAgeLimit')
  if (value.ageLimit !== 'SET' || value.maxAgeMillis === undefined) return t('tagUnknown')
  return t('tagHoursValue', { hours: value.maxAgeMillis / 3600000 })
}
function confirm() {
  if (!valid.value || !props.row || !props.capabilities || !expected.value) return
  // Capture exactly what was displayed. No defaults or source fields are re-inferred on submit.
  confirmation.value = {
    row: { ...props.row, scope: { ...props.row.scope }, target: { ...props.row.target }, retention: { ...props.row.retention } },
    expected: { ...expected.value },
    body: {
      scope: { ...props.row.scope },
      retention: { mode: form.mode, ...(form.mode === 'AGE_LIMIT' ? { maxAgeMillis: expected.value.maxAgeMillis } : {}) },
      expectedRetentionModel: props.capabilities.retention.model,
      expectedUpdateClock: props.capabilities.retention.updateClock || '',
      ...(form.mode === 'NATIVE_DEFAULT' ? { expectedRetentionConfigToken: props.capabilities.retention.configObservationToken } : {}),
    },
  }
}
async function submit() {
  const confirmed = confirmation.value
  if (busy.value || !confirmed || !valid.value) return
  busy.value = true; emit('pending', true); emit('start')
  try { await updateTagRetention(props.source, confirmed.row, confirmed.body, confirmed.expected); emit('saved') }
  catch (failure) { emit('failure', failure) }
  finally { busy.value = false; emit('pending', false); emit('close') }
}
</script>

<template>
  <a-modal :open="!!row" :title="$t(confirmation ? 'tagRetentionConfirmTitle' : 'tagUpdateRetention')" :closable="!busy" :mask-closable="false" :keyboard="!busy" @cancel="emit('close')">
    <p>{{ source.catalog }}.{{ source.db }}.{{ source.table }} · {{ row?.scope.branch || row?.scope.kind }}</p>
    <a-descriptions :column="1" bordered>
      <a-descriptions-item :label="$t('tagName')">{{ row?.name }}</a-descriptions-item>
      <a-descriptions-item :label="$t('snapshotId')">{{ row?.target.id }}</a-descriptions-item>
      <a-descriptions-item :label="$t('tagCurrentRetention')">{{ policyText(row?.retention) }}</a-descriptions-item>
      <a-descriptions-item v-if="confirmation" :label="$t('tagNewRetention')">{{ policyText(confirmation.expected) }}</a-descriptions-item>
    </a-descriptions>
    <a-form v-if="!confirmation" layout="vertical" class="g-mt-16">
      <a-form-item :label="$t('tagNewRetention')" name="retention-mode" html-for="retention-mode">
        <a-select id="retention-mode" v-model:value="form.mode" :aria-label="$t('tagNewRetention')" :placeholder="$t('tagChooseRetention')" :disabled="busy">
          <a-select-option v-for="mode in modes" :key="mode" :value="mode">{{ $t(`tagMode${mode}`) }}</a-select-option>
        </a-select>
      </a-form-item>
      <a-form-item v-if="form.mode === 'AGE_LIMIT'" :label="$t('tagHours')" name="retention-hours" html-for="retention-hours">
        <a-input-number id="retention-hours" v-model:value="form.hours" :aria-label="$t('tagHours')" :min="0" :disabled="busy" style="width: 100%" />
      </a-form-item>
      <p v-if="form.mode === 'NATIVE_DEFAULT'">{{ $t('tagDefaultResolved') }}：{{ policyText(capabilities?.retention.nativeDefault) }}</p>
    </a-form>
    <a-alert v-if="confirmation?.expected.ageLimit === 'SET'" type="warning" show-icon class="g-mt-16" :message="$t(confirmation.body.expectedUpdateClock === 'RESET_ON_UPDATE' ? 'tagRetentionResetNotice' : 'tagRetentionPreserveNotice')" />
    <p class="g-mt-16">{{ $t('tagRetentionMetadataOnly') }}</p>
    <template #footer>
      <a-button :disabled="busy" @click="emit('close')">{{ $t('cancel') }}</a-button>
      <a-button v-if="confirmation" :disabled="busy" @click="confirmation = null">{{ $t('tagBackEdit') }}</a-button>
      <a-button v-if="!confirmation" type="primary" :disabled="!valid" @click="confirm">{{ $t('tagNextConfirm') }}</a-button>
      <a-button v-else type="primary" :disabled="busy || !expected" :loading="busy" @click="submit">{{ $t('tagConfirmRetention') }}</a-button>
    </template>
  </a-modal>
</template>
