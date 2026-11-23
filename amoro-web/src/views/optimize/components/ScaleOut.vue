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
import { onMounted, reactive, ref } from 'vue'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import type { IGroupItem } from '@/types/common.type'
import { getOptimizerGroups, scaleoutResource } from '@/services/optimize.service'

interface FormState {
  resourceGroup: undefined | string
  parallelism: number
}

const props = defineProps<{ visible: boolean, resourceGroup: string }>()

const emit = defineEmits<{
  (e: 'cancel'): void
  (e: 'refreshOptimizersTab'): void
}>()

const confirmLoading = ref<boolean>(false)
const placeholder = reactive(usePlaceholder())
const formRef = ref()
const formState: FormState = reactive({
  resourceGroup: props.resourceGroup || undefined,
  parallelism: 1,
})

const groupList = reactive<IGroupItem[]>([])
async function getCompactQueues() {
  const result = await getOptimizerGroups()
  groupList.length = 0;
  (result || []).forEach((item: IGroupItem) => {
    groupList.push({
      ...item,
      label: item.optimizerGroupName,
      value: item.optimizerGroupName,
    })
  })
}

function handleOk() {
  formRef.value
    .validate()
    .then(async () => {
      confirmLoading.value = true
      await scaleoutResource({
        optimizerGroup: formState.resourceGroup || '',
        parallelism: Number(formState.parallelism),
      })
      formRef.value.resetFields()
      emit('cancel')
      emit('refreshOptimizersTab')
      confirmLoading.value = false
    })
    .catch(() => {
      confirmLoading.value = false
    })
}

function handleCancel() {
  formRef.value.resetFields()
  emit('cancel')
}
onMounted(() => {
  getCompactQueues()
})
</script>

<template>
  <el-dialog
    :model-value="props.visible"
    :title="$t('scaleOut')"
    :show-close="false"
    :width="520"
    @update:model-value="(open: boolean) => { if (!open) handleCancel() }"
  >
    <el-form ref="formRef" :model="formState" class="label-120" label-position="left">
      <el-form-item prop="resourceGroup" :label="$t('resourceGroup')" :rules="[{ required: true, message: `${placeholder.resourceGroupPh}` }]">
        <el-select
          v-model="formState.resourceGroup"
          :filterable="true"
          :options="groupList"
          :placeholder="placeholder.resourceGroupPh"
        />
      </el-form-item>
      <el-form-item prop="parallelism" :label="$t('parallelism')" :rules="[{ required: true, message: `${placeholder.parallelismPh}` }]">
        <el-input v-model="formState.parallelism" type="number" :placeholder="placeholder.parallelismPh" />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button :disabled="confirmLoading" @click="handleCancel">
        {{ $t('cancel') }}
      </el-button>
      <el-button type="primary" :loading="confirmLoading" @click="handleOk">
        {{ $t('ok') }}
      </el-button>
    </template>
  </el-dialog>
</template>
