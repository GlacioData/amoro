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
import { computed, onMounted, reactive, ref } from 'vue'
import { useI18n } from 'vue-i18n'
import { isValidParallelism, runExclusiveSubmit } from '../resource-state'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import { createOptimizerResource, getOptimizerGroups } from '@/services/optimize.service'

interface FormState {
  resourceGroup: string
  parallelism: number | undefined
}

const emit = defineEmits<{ (e: 'cancel'): void, (e: 'refresh'): void }>()

const { t } = useI18n()
const confirmLoading = ref<boolean>(false)
const placeholder = reactive(usePlaceholder())
const formRef = ref()
const formState: FormState = reactive({
  resourceGroup: '',
  parallelism: 1,
})

const parallelismRules = computed(() => [
  { required: true, message: `${placeholder.parallelismPh}` },
  {
    validator: (_rule: unknown, value: unknown, callback: (error?: Error) => void) => {
      if (!isValidParallelism(value)) {
        callback(new Error(t('parallelismPositiveIntTip')))
        return
      }
      callback()
    },
    trigger: ['blur', 'change'],
  },
])

const resourceGroupOptions = ref<{ label: string, value: string }[]>([])

async function loadResourceGroups() {
  try {
    const response = await getOptimizerGroups()
    resourceGroupOptions.value = (response || []).map((group: any) => ({
      label: group.optimizerGroupName,
      value: group.optimizerGroupName,
    }))
  }
  catch (error) {
    console.error('Failed to load resource groups:', error)
  }
}

async function handleOk() {
  await runExclusiveSubmit(
    () => confirmLoading.value,
    (busy) => { confirmLoading.value = busy },
    async () => {
      try {
        await formRef.value.validate()
      }
      catch {
        return
      }

      try {
        await createOptimizerResource({
          optimizerGroup: formState.resourceGroup || '',
          parallelism: Number(formState.parallelism),
        })
        formRef.value?.resetFields()
        emit('refresh')
      }
      catch {
        // Error feedback is handled by the request interceptor.
      }
    },
  )
}

function handleCancel() {
  formRef.value?.resetFields()
  emit('cancel')
}

onMounted(() => {
  loadResourceGroups()
})
</script>

<template>
  <el-dialog
    :model-value="true"
    :title="$t('createOptimizer')"
    :show-close="false"
    :width="520"
    @update:model-value="(open: boolean) => { if (!open) handleCancel() }"
  >
    <el-form ref="formRef" :model="formState" class="label-120" label-position="left">
      <el-form-item
        prop="resourceGroup"
        :label="$t('resourceGroup')"
        :rules="[{ required: true, message: `${placeholder.resourceGroupPh}` }]"
      >
        <el-select
          v-model="formState.resourceGroup"
          :placeholder="placeholder.resourceGroupPh"
        >
          <el-option
            v-for="option in resourceGroupOptions"
            :key="option.value"
            :label="option.label"
            :value="option.value"
          />
        </el-select>
      </el-form-item>
      <el-form-item
        prop="parallelism"
        :label="$t('parallelism')"
        :rules="parallelismRules"
      >
        <el-input-number
          v-model="formState.parallelism"
          :min="1"
          :step="1"
          :precision="0"
          controls-position="right"
          :placeholder="placeholder.parallelismPh"
          style="width: 100%"
        />
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
