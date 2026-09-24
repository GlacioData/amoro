<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

<script setup lang="ts">
import { QuestionCircleOutlined } from '@ant-design/icons-vue'
import { Tooltip as ATooltip } from 'ant-design-vue'
import { useI18n } from 'vue-i18n'
import type { ITableModel } from '@/types/common.type'
import { tableModelDescription } from '@/utils/tableModel'

// https://vuejs.org/guide/typescript/composition-api.html#typing-component-props
defineProps<{ format: string, model: ITableModel | null }>()
const { t } = useI18n()
</script>

<template>
  <p class="table-model">
    {{ t('tableModel') }}: <span class="text-color">{{ model?.code || '—' }}</span>
    <!-- Tooltip trigger must receive focus/mouse events: https://antdv.com/components/tooltip -->
    <ATooltip :title="t(tableModelDescription(format, model))" :trigger="['hover', 'focus']">
      <button type="button" class="model-help" :aria-label="t('tableModelHelp')">
        <QuestionCircleOutlined />
      </button>
    </ATooltip>
  </p>
</template>

<style scoped>
.table-model {
  display: inline-flex;
  align-items: center;
  gap: 4px;
}
.text-color {
  color: #7CB305;
}
.model-help {
  padding: 0;
  border: 0;
  background: transparent;
  color: inherit;
  cursor: help;
  line-height: 1;
}
.model-help:focus-visible {
  outline: 2px solid #1677ff;
  outline-offset: 2px;
}
</style>
