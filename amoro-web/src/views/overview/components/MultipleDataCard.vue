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
import { useI18n } from 'vue-i18n'
import type { IKeyAndValue } from '@/types/common.type'

const props = defineProps<{ title: string, data: IKeyAndValue<string | number>[] }>()

const { t } = useI18n()
</script>

<template>
  <el-card>
    <template #header>
      <span class="card-title">{{ props.title }}</span>
    </template>
    <div class="resource-statistics">
      <el-statistic
        v-for="item in props.data"
        :key="item.key"
        :title="t(item.key)"
        :value="typeof item.value === 'number' ? item.value : undefined"
        :formatter="typeof item.value === 'string' ? () => item.value : undefined"
      />
    </div>
  </el-card>
</template>

<style scoped>
.resource-statistics {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(min(100%, 100px), 1fr));
  gap: 12px;
}
</style>
