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
import { Close } from '@element-plus/icons-vue'
import { computed, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { tableColumnProps } from '@/utils/table'
import type { IMap } from '@/types/common.type'
import { getUUid } from '@/utils/index'

interface Propertie {
  key: string
  value: string
  uuid: string
}

const props = defineProps<{ propertiesObj: IMap<string>, isEdit: boolean }>()
const { t } = useI18n()
const propertiesColumns = computed(() => [
  { dataIndex: 'key', title: t('key'), width: 284, ellipsis: true },
  { dataIndex: 'value', title: t('value'), ellipsis: true },
])
const propertiesFormRef = ref()
const propertiesForm = reactive<{ data: Propertie[] }>({
  data: [],
})

const rules = [{
  required: true,
  message: ``,
}]

const isEdit = computed(() => props.isEdit)

watch(() => props.propertiesObj, () => {
  initPropertiesArray()
}, {
  immediate: true,
  deep: true,
})

function initPropertiesArray() {
  propertiesForm.data.length = 0
  Object.keys(props.propertiesObj).forEach((key) => {
    propertiesForm.data.push({
      key,
      value: props.propertiesObj[key],
      uuid: getUUid(),
    })
  })
}

function removeRule(item: Propertie) {
  const index = propertiesForm.data.indexOf(item)
  if (index !== -1) {
    propertiesForm.data.splice(index, 1)
  }
  // propertiesFormRef.value.validate()
}
function addRule() {
  propertiesForm.data.push({
    key: '',
    value: '',
    uuid: getUUid(),
  })
}

// async function validateUnique(rule, value) {
//   if (!value) {
//     return Promise.reject(new Error(t(placeholder.inputPh)))
//   } else if (value && (propertiesForm.data || []).filter(item => item.key === value).length > 1) {
//     return Promise.reject(new Error(t('duplicateKey')))
//   } else {
//     return Promise.resolve()
//   }
// }

defineExpose({
  getPropertiesWithoputValidation() {
    const propObj: IMap<string> = {}
    propertiesForm.data.forEach((e) => {
      propObj[e.key] = e.value
    })
    return Promise.resolve(propObj)
  },
  getProperties() {
    return propertiesFormRef.value
      .validate()
      .then(() => {
        const propObj: IMap<string> = {}
        propertiesForm.data.forEach((e) => {
          propObj[e.key] = e.value
        })
        return Promise.resolve(propObj)
      })
      .catch(() => {
        return false
      })
  },
})
</script>

<template>
  <div class="config-properties">
    <div v-if="isEdit">
      <div class="config-header g-flex">
        <div class="td g-flex-ac">
          {{ $t('key') }}
        </div>
        <div class="td g-flex-ac bd-left">
          {{ $t('value') }}
        </div>
      </div>
      <el-form ref="propertiesFormRef" inline :model="propertiesForm" class="g-mt-12" label-position="left">
        <div v-for="(item, index) in propertiesForm.data" :key="item.uuid" class="config-row">
          <!-- validator: validateUnique -->
          <el-form-item :prop="['data', index, 'key']" :rules="rules">
            <el-input v-model="item.key" style="width: 100%" />
          </el-form-item>
          <el-form-item :prop="['data', index, 'value']" :rules="rules">
            <el-input v-model="item.value" style="width: 100%" />
          </el-form-item>
          <Close class="amoro-icon icon-close" @click="removeRule(item)" />
        </div>
      </el-form>
      <el-button class="config-btn" @click="addRule">
        +
      </el-button>
    </div>
    <el-table v-if="!isEdit" row-key="uuid" :data="propertiesForm.data">
      <el-table-column v-for="column in propertiesColumns" :key="column.dataIndex" v-bind="tableColumnProps(column, propertiesColumns)" />
    </el-table>
  </div>
</template>

<style lang="less">
.config-properties {
  min-width: 0;
  width: 100%;
  display: flex;
  flex-direction: column;
  line-height: 32px;

  .config-header {
    box-sizing: border-box;
    width: 100%;
    padding-right: 32px;
    background: #fafafa;
    border-bottom: 1px solid #e8e8f0;

    .td {
      width: 50%;
      height: 40px;
      padding: 8px 12px;
      color: #102048;
      font-weight: 500;
    }

    .bd-left {
      position: relative;

      &:before {
        position: absolute;
        top: 50%;
        left: 0;
        width: 1px;
        height: 1.6em;
        background-color: rgba(0, 0, 0, 0.06);
        transform: translateY(-50%);
        transition: background-color 0.3s;
        content: '';
      }
    }
  }

  .config-row {
    box-sizing: border-box;
    gap: 10px;
    display: flex;
    position: relative;
    padding-right: 10px;
    width: 100%;
    align-items: center;
    margin-bottom: 10px;

    .el-form-item {
      flex: 1;
      min-width: 0;
      margin: 0px !important;
    }

    .icon-close {
      cursor: pointer;
      font-size: 12px;

      &.disabled {
        cursor: not-allowed;
      }
    }
  }

  .config-btn {
    border-radius: 4px;
    width: 100%;
    border: 1px solid #e5e5e5;
    text-align: center;
    color: #102048;
    box-shadow: none;
  }
}
</style>
