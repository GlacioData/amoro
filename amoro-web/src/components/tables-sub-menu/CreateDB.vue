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

<script lang="ts">
import { defineComponent, reactive, ref } from 'vue'
import { usePlaceholder } from '@/hooks/usePlaceholder'

interface FormState {
  catalog: string | undefined
  dbname: string
}

export default defineComponent({
  props: {
    visible: {
      type: Boolean,
      default: false,
    },
    catalogOptions: {
      type: Array,
      default: () => [],
    },
  },
  emits: ['cancel'],
  setup(_, { emit }) {
    const placeholder = reactive(usePlaceholder())

    const formRef = ref()
    const formState: FormState = reactive({
      catalog: undefined,
      dbname: '',
    })
    const handleOk = () => {
      formRef.value
        .validate()
        .then(() => {
          formRef.value.resetFields()
          emit('cancel')
        })
        .catch((info: Error) => {
          // eslint-disable-next-line no-console
          console.log('Validate Failed:', info)
        })
    }
    const handleCancel = () => {
      formRef.value.resetFields()
      emit('cancel')
    }

    return {
      formRef,
      formState,
      placeholder,
      handleOk,
      handleCancel,
    }
  },
})
</script>

<template>
  <el-dialog :model-value="visible" :title="$t('createDatabase')" :width="520" @update:model-value="(open: boolean) => { if (!open) handleCancel() }">
    <el-form ref="formRef" :model="formState" class="label-120" label-position="left">
      <el-form-item prop="catalog" :label="$t('catalog')" :rules="[{ required: true, message: `${placeholder.selectClPh}` }]">
        <el-select
          v-model="formState.catalog"
          :options="catalogOptions"
          :placeholder="placeholder.selectClPh"
        />
      </el-form-item>
      <el-form-item prop="dbname" :label="$t('databaseName')" :rules="[{ required: true, message: `${placeholder.inputDBPh}` }]">
        <el-input v-model="formState.dbname" :placeholder="placeholder.inputDBPh" />
      </el-form-item>
    </el-form>
    <template #footer>
      <el-button @click="handleCancel">
        {{ $t('cancel') }}
      </el-button>
      <el-button type="primary" @click="handleOk">
        {{ $t('ok') }}
      </el-button>
    </template>
  </el-dialog>
</template>
