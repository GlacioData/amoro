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
import { computed, onMounted, reactive, ref, watch } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRoute, useRouter } from 'vue-router'
import type { UploadFile, UploadUserFile } from 'element-plus'
import Properties from './Properties.vue'
import { confirmAction, message, warnAction } from '@/utils/ui-feedback'
import { checkCatalogStatus, delCatalog, getCatalogsSetting, getCatalogsTypes, getMetastoreStorageTypes, getMetastoreTableFormats, saveCatalogsSetting } from '@/services/setting.services'
import type { ICatalogItem, IIOptimizeGroupItem, ILableAndValue, IMap } from '@/types/common.type'
import { usePlaceholder } from '@/hooks/usePlaceholder'
import { getResourceGroupsListAPI } from '@/services/optimize.service'
import { canManageCatalog } from '@/utils/permission'

interface IStorageConfigItem {
  label: string
  value: string
  key: string
  fileName: string
  fileId: string
  fileList: UploadUserFile[]
  uploadLoading: boolean
  isSuccess: boolean
}

interface FormState {
  catalog: IMap<string | undefined>
  tableFormatList: string[]
  storageConfig: IMap<string>
  authConfig: IMap<string>
  properties: IMap<string>
  tableProperties: IMap<string>
  storageConfigArray: IStorageConfigItem[]
  authConfigArray: IStorageConfigItem[]
}

const props = defineProps<{ isEdit: boolean }>()

const emit = defineEmits<{
  (e: 'updateEdit', val: boolean, catalog?: ICatalogItem): void
  (e: 'updateCatalogs'): void
}>()

const formState: FormState = reactive({
  catalog: {
    name: '',
    type: 'ams',
    optimizerGroup: undefined,
  },
  tableFormatList: [],
  storageConfig: {},
  authConfig: {},
  properties: {},
  tableProperties: {},
  storageConfigArray: [],
  authConfigArray: [],
})

const { t } = useI18n()
const router = useRouter()
const route = useRoute()
const placeholder = reactive(usePlaceholder())
const metastoreType = ref<string>('')
const isEdit = computed(() => {
  return props.isEdit
})
const uploadUrl = computed(() => {
  return '/api/ams/v1/files'
})
const uploadHeaders = computed(() => {
  return {
    'X-Request-Source': 'Web',
  }
})
const isNewCatalog = computed(() => {
  const catalog = (route.query?.catalogname || '').toString()
  return decodeURIComponent(catalog) === 'new catalog'
})
// Arctic Metastore supports Mixed Iceberg format only.
// Hive Metastore supports Iceberg and Mixed Hive format only.
// Hadoop and Custom support Iceberg format only.
const isHiveMetastore = computed(() => {
  return formState.catalog.type === 'hive'
})
const loading = ref<boolean>(false)
const writable = computed(() => canManageCatalog())
const formRef = ref()
const propertiesRef = ref()
const tablePropertiesRef = ref()
// ICEBERG  MIXED_HIVE  MIXED_ICEBERG
const tableFormatMap = {
  MIXED_HIVE: 'MIXED_HIVE',
  ICEBERG: 'ICEBERG',
  MIXED_ICEBERG: 'MIXED_ICEBERG',
  PAIMON: 'PAIMON',
}

const tableFormatText = {
  [tableFormatMap.ICEBERG]: 'Iceberg',
  [tableFormatMap.MIXED_HIVE]: 'Mixed Hive',
  [tableFormatMap.MIXED_ICEBERG]: 'Mixed Iceberg',
  [tableFormatMap.PAIMON]: 'Paimon',
}

const storageConfigFileNameMap = {
  'hadoop.core.site': 'core-site.xml',
  'hadoop.hdfs.site': 'hdfs-site.xml',
  'hive.site': 'hive-site.xml',
}
const newCatalogConfig = {
  storageConfig: {
    'hadoop.core.site': '',
    'hadoop.hdfs.site': '',
  },
  authConfig: {
    'auth.kerberos.keytab': '',
    'auth.kerberos.krb5': '',
  },
}

const hadoopConfigTypeOps = reactive<ILableAndValue[]>([{
  label: 'SIMPLE',
  value: 'SIMPLE',
}, {
  label: 'KERBEROS',
  value: 'KERBEROS',
}])

const s3ConfigTypeOps = reactive<ILableAndValue[]>([{
  label: 'AK/SK',
  value: 'AK/SK',
}, {
  label: 'CUSTOM',
  value: 'CUSTOM',
}])

const ossConfigTypeOps = reactive<ILableAndValue[]>([{
  label: 'AK/SK',
  value: 'AK/SK',
}, {
  label: 'CUSTOM',
  value: 'CUSTOM',
}])

const storageConfigMap = {
  'hadoop.core.site': 'Hadoop core-site',
  'hadoop.hdfs.site': 'Hadoop hdfs-site',
  'hive.site': 'Hadoop hive-site',
}
const authConfigMap = {
  'auth.kerberos.keytab': 'Kerberos Keytab',
  'auth.kerberos.krb5': 'Kerberos Krb5',
}

const defaultPropertiesMap = {
  ams: ['warehouse'],
  hadoop: ['warehouse'],
  filesystem: ['warehouse'],
  custom: ['catalog-impl'],
  glue: ['warehouse'],
  rest: ['uri'],
  PAIMON: ['warehouse'],
}

let configRequest = 0
watch(() => route.query, (value) => {
  value && initData()
}, {
  immediate: true,
  deep: true,
})
const catalogTypeOps = reactive<ILableAndValue[]>([])
const storageTypeOptions = ref<ILableAndValue[]>([])
const tableFormatOptions = ref<string[]>([])
const optimizerGroupList = ref<ILableAndValue[]>([])
function initData() {
  getConfigInfo()
}
async function getOptimizerGroupList() {
  const res = await getResourceGroupsListAPI()
  const list = (res || []).map((item: IIOptimizeGroupItem) => ({ lable: item.resourceGroup.name, value: item.resourceGroup.name }))

  if (list.length === 0) {
    confirmAction({
      title: t('noResourceGroupsTitle'),
      content: t('noResourceGroupsContent'),
      okText: t('goToButtonText'),
      onOk: async () => {
        try {
          router.push({ path: '/optimizing', query: { tab: 'optimizerGroups' } })
        }
        catch (error) {
          console.error('Navigation error:', error)
        }
      },
    })
  }
  else {
    optimizerGroupList.value = list
  }
}
async function getCatalogTypeOps() {
  const res = await getCatalogsTypes()
  catalogTypeOps.length = 0
  ;(res || []).forEach((ele: any) => {
    catalogTypeOps.push({
      label: ele.display,
      value: ele.value,
    })
  })
  getMetastoreType()
}
function getMetastoreType() {
  metastoreType.value = (catalogTypeOps.find(ele => ele.value === formState.catalog.type) || {}).label || ''
}
async function loadMetastoreCapabilities(loadTableFormats: boolean) {
  const type = formState.catalog.type as string
  if (!type) {
    return
  }
  const formats = await getMetastoreTableFormats(type)
  const options = (formats || []) as string[]
  tableFormatOptions.value = options
  if (loadTableFormats) {
    formState.tableFormatList = [...options]
  }
  const storageTypes = await getMetastoreStorageTypes(type)
  storageTypeOptions.value = (storageTypes || []).map((val: string) => ({
    label: val,
    value: val,
  }))
  const availableTypes = storageTypeOptions.value.map(option => option.value)
  const currentStorageType = formState.storageConfig['storage.type']
  if (!currentStorageType || !availableTypes.includes(currentStorageType)) {
    formState.storageConfig['storage.type'] = availableTypes.length > 0 ? availableTypes[0] : ''
  }
}
async function getConfigInfo() {
  const request = ++configRequest
  try {
    loading.value = true
    const { catalogname, type } = route.query
    if (!catalogname)
      return

    if (isNewCatalog.value) {
      formState.catalog.name = ''
      formState.catalog.type = (type || 'ams') as string
      formState.catalog.optimizerGroup = undefined
      formState.tableFormatList = []
      formState.authConfig = { ...newCatalogConfig.authConfig }
      formState.storageConfig = { ...newCatalogConfig.storageConfig }
      const keys = defaultPropertiesMap[formState.catalog.type as keyof typeof defaultPropertiesMap] || []
      formState.properties = {}
      keys.forEach((key) => {
        formState.properties[key] = ''
      })
      formState.tableProperties = {}
      formState.storageConfigArray.length = 0
      formState.authConfigArray.length = 0
      await changeMetastore()
    }
    else {
      const res = await getCatalogsSetting(catalogname as string)
      if (!res || request !== configRequest)
        return

      const { name, type, tableFormatList, storageConfig, authConfig, properties, tableProperties, optimizerGroup } = res
      formState.catalog.name = name
      formState.catalog.type = type
      formState.catalog.optimizerGroup = optimizerGroup
      formState.tableFormatList = [...tableFormatList]
      formState.authConfig = authConfig
      formState.storageConfig = storageConfig
      formState.properties = properties || {}
      formState.tableProperties = tableProperties || {}
      formState.storageConfigArray.length = 0
      formState.authConfigArray.length = 0
      getMetastoreType()
      await loadMetastoreCapabilities(false)
    }

    if (request !== configRequest)
      return
    const { storageConfig, authConfig } = formState
    Object.keys(storageConfig).forEach((key) => {
      const configArr = ['hadoop.core.site', 'hadoop.hdfs.site']
      if (isHiveMetastore.value) {
        configArr.push('hive.site')
      }
      if (configArr.includes(key)) {
        const item: IStorageConfigItem = {
          key,
          label: storageConfigMap[key as keyof typeof storageConfigMap],
          value: (storageConfig[key] as any)?.fileName,
          fileName: (storageConfig[key] as any)?.fileName,
          fileId: '',
          fileList: [],
          uploadLoading: false,
          isSuccess: false,
        }
        formState.storageConfigArray.push(item)
      }
    })
    Object.keys(authConfig).forEach((key) => {
      if (['auth.kerberos.keytab', 'auth.kerberos.krb5'].includes(key)) {
        const item: IStorageConfigItem = {
          key,
          label: authConfigMap[key as keyof typeof authConfigMap],
          value: (authConfig[key] as any)?.fileName,
          fileName: (authConfig[key] as any)?.fileName,
          fileId: '',
          fileList: [],
          uploadLoading: false,
          isSuccess: false,
        }
        formState.authConfigArray.push(item)
      }
    })
  }
  catch (error) {
  }
  finally {
    if (request === configRequest)
      loading.value = false
  }
}

async function changeProperties() {
  const properties = await propertiesRef.value.getPropertiesWithoputValidation()
  const catalogKeys = defaultPropertiesMap[formState.catalog.type as keyof typeof defaultPropertiesMap] || []
  catalogKeys.forEach((key) => {
    if (key && !properties[key]) {
      properties[key] = ''
    }
  })
  const formatKeys = formState.tableFormatList.reduce((accumulator, current) => {
    return [...accumulator, ...(defaultPropertiesMap[current as keyof typeof defaultPropertiesMap] || [])]
  }, [] as string[])
  formatKeys.forEach((key) => {
    if (key && !properties[key]) {
      properties[key] = ''
    }
  })
  for (const key in properties) {
    if (!properties[key] && !catalogKeys.includes(key) && !formatKeys.includes(key)) {
      delete properties[key]
    }
  }
  formState.properties = properties
}

const authTypeOptions = computed(() => {
  const type = formState.storageConfig['storage.type']
  if (type === 'Hadoop') {
    return hadoopConfigTypeOps
  }
  else if (type === 'S3') {
    return s3ConfigTypeOps
  }
  else if (type === 'OSS') {
    return ossConfigTypeOps
  }

  return null
})

const isAuthDisabled = computed(() => formState.storageConfig['storage.type'] === 'Local')

watch(
  () => formState.storageConfig['storage.type'],
  (storageType) => {
    if (storageType === 'Local') {
      if (isNewCatalog.value) {
        formState.tableFormatList = [tableFormatMap.ICEBERG]
      }
      if (!formState.authConfig['auth.type']) {
        formState.authConfig['auth.type'] = 'SIMPLE'
      }
      const simpleUsernameKey = 'auth.simple.hadoop_username'
      if (!formState.authConfig[simpleUsernameKey]) {
        formState.authConfig[simpleUsernameKey] = 'local'
      }
    }
  },
)

async function changeMetastore() {
  await loadMetastoreCapabilities(isNewCatalog.value)

  const index = formState.storageConfigArray.findIndex(item => item.key === 'hive.site')
  if (isHiveMetastore.value) {
    if (index > -1) {
      return
    }
    formState.storageConfigArray.push({
      key: 'hive.site',
      label: storageConfigMap['hive.site'],
      value: '',
      fileName: '',
      fileId: '',
      fileList: [],
      uploadLoading: false,
      isSuccess: false,
    })
    formState.storageConfig['hive.site'] = ''
  }
  else {
    if (index > -1) {
      formState.storageConfigArray.splice(index, 1)
      delete formState.storageConfig['hive.site']
    }
  }
  await changeProperties()
}

function handleEdit() {
  emit('updateEdit', true)
}
async function handleRemove() {
  const res = await checkCatalogStatus(formState.catalog.name as string)
  if (res) {
    deleteCatalogModal()
    return
  }
  warnAction({
    title: t('cannotDeleteModalTitle'),
    content: t('cannotDeleteModalContent'),
  })
}
async function validatorName(_: any, value: string) {
  if (!value) {
    return Promise.reject(new Error(t('inputPlaceholder')))
  }
  if ((/^[a-z][\w-]*$/i.test(value))) {
    return Promise.resolve()
  }
  else {
    return Promise.reject(new Error(t('invalidInput')))
  }
}
function getFileIdParams() {
  const { storageConfig, authConfig, storageConfigArray, authConfigArray } = formState
  Object.keys(authConfig).forEach((key) => {
    if (['auth.kerberos.keytab', 'auth.kerberos.krb5'].includes(key)) {
      const id = (authConfigArray.find(item => item.key === key) || {}).fileId
      authConfig[key as keyof typeof authConfig] = id || ''
    }
  })
  Object.keys(storageConfig).forEach((key) => {
    if (['hadoop.core.site', 'hadoop.hdfs.site', 'hive.site'].includes(key)) {
      const id = (storageConfigArray.find(item => item.key === key) || {}).fileId
      storageConfig[key as keyof typeof storageConfig] = id || ''
    }
  })
}
function handleSave() {
  formRef.value
    .validate()
    .then(async () => {
      const { catalog, tableFormatList, storageConfig, authConfig } = formState
      const properties = await propertiesRef.value.getProperties()
      const tableProperties = await tablePropertiesRef.value.getProperties()
      if (!properties) {
        return
      }
      if (!tableProperties) {
        return
      }
      loading.value = true
      const catalogParams = catalog
      getFileIdParams()
      await saveCatalogsSetting({
        isCreate: isNewCatalog.value,
        ...catalogParams,
        tableFormatList,
        storageConfig,
        authConfig,
        properties,
        tableProperties,
      } as any).then(() => {
        message.success(`${t('save')} ${t('success')}`)
        emit('updateEdit', false, {
          catalogName: catalog.name || '',
          catalogType: catalog.type || '',
        })
        getConfigInfo()
        formRef.value.resetFields()
      }).catch(() => {
        message.error(`${t('save')} ${t('failed')}`)
      }).finally(() => {
        loading.value = false
      })
    })
    .catch(() => {
    })
}
function handleCancel() {
  formRef.value.resetFields()
  emit('updateEdit', false)
  getConfigInfo()
}
async function deleteCatalogModal() {
  confirmAction({
    title: t('deleteCatalogModalTitle'),
    onOk: async () => {
      try {
        await delCatalog(formState.catalog.name || '')
        message.success({
          message: `${t('remove')} ${t('success')}`,
          duration: 1000,
          onClose: () => { void router.replace({ path: '/catalogs', query: {} }).then(() => router.go(0)) },
        })
      }
      catch (error) {
        message.error(`${t('remove')} ${t('failed')}`)
      }
    },
  })
}
// Element Plus reports HTTP success separately from AMS's business status.
// https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/upload.md
function uploadSucceeded(response: unknown, file: UploadFile, config: IStorageConfigItem, storage = false) {
  config.uploadLoading = false
  const result = response as { code?: number, message?: string, result?: { id?: string } } | null
  if (result?.code !== 200 || !result.result?.id) {
    message.error(result?.message || `${file.name} ${t('uploaded')} ${t('failed')}`)
    return
  }
  config.isSuccess = true
  config.fileName = storage ? storageConfigFileNameMap[config.key as keyof typeof storageConfigFileNameMap] : file.name
  config.fileId = result.result.id
  message.success(`${file.name} ${t('uploaded')} ${t('success')}`)
}

function uploadFailed(file: UploadFile, config: IStorageConfigItem) {
  config.uploadLoading = false
  config.isSuccess = false
  message.error(`${file.name} ${t('uploaded')} ${t('failed')}`)
}
onMounted(() => {
  getCatalogTypeOps()
  getOptimizerGroupList()
})
</script>

<template>
  <div class="detail-wrap">
    <div class="detail-content-wrap">
      <div class="content-wrap">
        <el-form ref="formRef" :model="formState" class="catalog-form" label-position="left">
          <el-form-item>
            <p class="header">
              {{ $t('basic') }}
            </p>
          </el-form-item>
          <el-form-item
            :label="$t('name')"
            :prop="['catalog', 'name']"
            :rules="[{ required: isEdit && isNewCatalog, validator: validatorName }]"
          >
            <el-input v-if="isEdit && isNewCatalog" v-model="formState.catalog.name" />
            <span v-else class="config-value">{{ formState.catalog.name }}</span>
          </el-form-item>
          <el-form-item
            :label="$t('metastore')"
            :prop="['catalog', 'type']"
            :rules="[{ required: isEdit && isNewCatalog }]"
          >
            <el-select
              v-if="isEdit && isNewCatalog"
              v-model="formState.catalog.type"
              :options="catalogTypeOps"
              :placeholder="placeholder.selectPh"
              @change="changeMetastore"
            />
            <span v-else>{{ metastoreType }}</span>
          </el-form-item>
          <el-form-item
            :label="$t('tableFormat')"
            :prop="['tableFormatList']"
          >
            <el-checkbox-group
              v-if="isEdit"
              v-model="formState.tableFormatList"
            >
              <el-checkbox
                v-for="item in tableFormatOptions"
                :key="item"
                :value="item"
              >
                {{ tableFormatText[item] || item }}
              </el-checkbox>
            </el-checkbox-group>
            <span v-else class="config-value">
              {{ formState.tableFormatList.map((item) => tableFormatText[item] || item).join(', ') }}
            </span>
          </el-form-item>
          <el-form-item
            :label="$t('optimizerGroup')"
            :prop="['catalog', 'optimizerGroup']"
            :rules="[{ required: isEdit }]"
          >
            <el-select
              v-if="isEdit"
              v-model="formState.catalog.optimizerGroup"
              :options="optimizerGroupList"
              :placeholder="placeholder.selectPh"
            />
            <span v-else>{{ formState.catalog.optimizerGroup }}</span>
          </el-form-item>
          <el-form-item>
            <p class="header">
              {{ $t('storageConfigName') }}
            </p>
          </el-form-item>
          <el-form-item :label="$t('type')" :prop="['storageConfig', 'storage.type']" :rules="[{ required: isEdit }]">
            <el-select
              v-if="isEdit"
              v-model="formState.storageConfig['storage.type']"
              :placeholder="placeholder.selectPh"
              :options="storageTypeOptions"
            />
            <span v-else class="config-value">{{ formState.storageConfig['storage.type'] }}</span>
          </el-form-item>
          <el-form-item
            v-if="formState.storageConfig['storage.type'] === 'S3'"
            :label="$t('endpoint')"
            :prop="['storageConfig', 'storage.s3.endpoint']"
            :rules="[{ required: false }]"
          >
            <el-input v-if="isEdit" v-model="formState.storageConfig['storage.s3.endpoint']" />
            <span v-else class="config-value">{{ formState.storageConfig['storage.s3.endpoint'] }}</span>
          </el-form-item>
          <el-form-item
            v-if="formState.storageConfig['storage.type'] === 'S3'"
            :label="$t('region')"
            :prop="['storageConfig', 'storage.s3.region']"
            :rules="[{ required: false }]"
          >
            <el-input v-if="isEdit" v-model="formState.storageConfig['storage.s3.region']" />
            <span v-else class="config-value">{{ formState.storageConfig['storage.s3.region'] }}</span>
          </el-form-item>
          <el-form-item
            v-if="formState.storageConfig['storage.type'] === 'OSS'"
            :label="$t('endpoint')"
            :prop="['storageConfig', 'storage.oss.endpoint']"
            :rules="[{ required: false }]"
          >
            <el-input v-if="isEdit" v-model="formState.storageConfig['storage.oss.endpoint']" />
            <span v-else class="config-value">{{ formState.storageConfig['storage.oss.endpoint'] }}</span>
          </el-form-item>
          <div v-if="formState.storageConfig['storage.type'] === 'Hadoop'">
            <el-form-item
              v-for="config in formState.storageConfigArray"
              :key="config.label"
              :label="config.label"
              class="g-flex-ac"
            >
              <el-upload
                v-if="isEdit"
                v-model:file-list="config.fileList"
                name="file"
                accept=".xml"
                :show-upload-list="false"
                :action="uploadUrl"
                :disabled="config.uploadLoading"
                :headers="uploadHeaders"
                :before-upload="() => { config.uploadLoading = true; return true }"
                :on-success="(response: unknown, file: UploadFile) => uploadSucceeded(response, file, config, true)"
                :on-error="(_: Error, file: UploadFile) => uploadFailed(file, config)"
              >
                <el-button type="primary" plain :loading="config.uploadLoading" class="g-mr-12">
                  {{ $t('upload')
                  }}
                </el-button>
              </el-upload>
              <span v-if="config.isSuccess || config.fileName" class="config-value">{{ config.fileName }}</span>
            </el-form-item>
          </div>
          <el-form-item>
            <p class="header">
              {{ $t('authenticationConfig') }}
            </p>
          </el-form-item>
          <el-form-item :label="$t('type')" :prop="['authConfig', 'auth.type']" :rules="[{ required: isEdit && !isAuthDisabled }]">
            <el-select
              v-if="isEdit"
              v-model="formState.authConfig['auth.type']"
              :placeholder="placeholder.selectPh"
              :options="authTypeOptions"
              :disabled="isAuthDisabled"
            />
            <span v-else class="config-value">{{ formState.authConfig['auth.type'] }}</span>
          </el-form-item>
          <el-form-item
            v-if="formState.authConfig['auth.type'] === 'SIMPLE'"
            :label="$t('hadoopUsername')"
            :prop="['authConfig', 'auth.simple.hadoop_username']"
            :rules="[{ required: isEdit && !isAuthDisabled }]"
          >
            <el-input v-if="isEdit" v-model="formState.authConfig['auth.simple.hadoop_username']" :disabled="isAuthDisabled" />
            <span v-else class="config-value">{{ formState.authConfig['auth.simple.hadoop_username'] }}</span>
          </el-form-item>
          <el-form-item
            v-if="formState.authConfig['auth.type'] === 'KERBEROS'"
            :label="$t('kerberosPrincipal')"
            :prop="['authConfig', 'auth.kerberos.principal']"
            :rules="[{ required: isEdit && !isAuthDisabled }]"
          >
            <el-input v-if="isEdit" v-model="formState.authConfig['auth.kerberos.principal']" :disabled="isAuthDisabled" />
            <span v-else class="config-value">{{ formState.authConfig['auth.kerberos.principal'] }}</span>
          </el-form-item>
          <div v-if="formState.authConfig['auth.type'] === 'KERBEROS'">
            <el-form-item
              v-for="config in formState.authConfigArray"
              :key="config.label"
              :label="config.label"
              class="g-flex-ac"
            >
              <el-upload
                v-if="isEdit"
                v-model:file-list="config.fileList"
                name="file"
                :accept="config.key === 'auth.kerberos.keytab' ? '.keytab' : '.conf'"
                :show-upload-list="false"
                :action="uploadUrl"
                :disabled="config.uploadLoading"
                :headers="uploadHeaders"
                :before-upload="() => { config.uploadLoading = true; return true }"
                :on-success="(response: unknown, file: UploadFile) => uploadSucceeded(response, file, config)"
                :on-error="(_: Error, file: UploadFile) => uploadFailed(file, config)"
              >
                <el-button type="primary" plain :loading="config.uploadLoading" class="g-mr-12">
                  {{ $t('upload')
                  }}
                </el-button>
              </el-upload>
              <span v-if="config.isSuccess || config.fileName" class="config-value auth-filename" :title="config.fileName">
                {{ config.fileName }}
              </span>
            </el-form-item>
          </div>
          <el-form-item
            v-if="formState.authConfig['auth.type'] === 'AK/SK'"
            :label="$t('accessKey')"
            :prop="['authConfig', 'auth.ak_sk.access_key']"
            :rules="[{ required: isEdit && !isAuthDisabled }]"
          >
            <el-input v-if="isEdit" v-model="formState.authConfig['auth.ak_sk.access_key']" :disabled="isAuthDisabled" />
            <span v-else class="config-value">{{ formState.authConfig['auth.ak_sk.access_key'] }}</span>
          </el-form-item>
          <el-form-item
            v-if="formState.authConfig['auth.type'] === 'AK/SK'"
            :label="$t('secretKey')"
            :prop="['authConfig', 'auth.ak_sk.secret_key']"
            :rules="[{ required: isEdit && !isAuthDisabled }]"
          >
            <el-input v-if="isEdit" v-model="formState.authConfig['auth.ak_sk.secret_key']" :disabled="isAuthDisabled" />
            <span v-else class="config-value">{{ formState.authConfig['auth.ak_sk.secret_key'] }}</span>
          </el-form-item>
          <el-form-item>
            <p class="header">
              {{ $t('properties') }}
            </p>
          </el-form-item>
          <el-form-item>
            <Properties ref="propertiesRef" :properties-obj="formState.properties" :is-edit="isEdit" />
          </el-form-item>
          <el-form-item>
            <p class="header">
              {{ $t('tableProperties') }}
            </p>
          </el-form-item>
          <el-form-item>
            <Properties ref="tablePropertiesRef" :properties-obj="formState.tableProperties" :is-edit="isEdit" />
          </el-form-item>
        </el-form>
      </div>
    </div>
    <div v-if="isEdit && writable" class="footer-btn">
      <el-button type="primary" class="save-btn g-mr-12" @click="handleSave">
        {{ $t('save') }}
      </el-button>
      <el-button @click="handleCancel">
        {{ $t('cancel') }}
      </el-button>
    </div>
    <div v-if="!isEdit && writable" class="footer-btn">
      <el-button type="primary" class="edit-btn g-mr-12" @click="handleEdit">
        {{ $t('edit') }}
      </el-button>
      <el-button class="remove-btn" @click="handleRemove">
        {{ $t('remove') }}
      </el-button>
    </div>
    <u-loading v-if="loading" />
  </div>
</template>

<style lang="less" scoped>
.detail-wrap {
  height: 100%;
  padding: 16px 0 16px 24px;
  display: flex;
  flex: 1;
  flex-direction: column;
  border: 1px solid #e8e8f0;
  border-left: 0;

  .detail-content-wrap {
    height: 100%;
    padding-right: 200px;
    overflow: auto;
  }

  .content-wrap {
    display: flex;
    flex: 1;
    overflow: auto;
    flex-direction: column;

    .el-form-item {
      margin-bottom: 8px;
    }

    :deep(.el-form-item__label) {
      >label {
        word-break: break-all;
        white-space: pre-wrap;
      }

      width: 280px;
      margin-right: 16px;
    }

    .header {
      font-size: 16px;
      font-weight: 600;
      color: #102048;
    }

    .config-value {
      word-break: break-all;

      &.auth-filename {
        max-width: 72%;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
        display: inline-block;
        vertical-align: middle;
        margin-top: -4px;
      }
    }
  }

  .footer-btn {
    height: 44px;
    flex-shrink: 0;
    padding-top: 12px;
    background-color: #fff;

    .edit-btn,
    .save-btn {
      min-width: 60px;
    }

    .remove-btn {
      &:hover {
        background-color: #ff4d4f;
        border-color: transparent;
        color: #fff;
      }
    }
  }
}

@media (max-width: 1200px) {
  .detail-wrap {
    min-width: 0;
    padding-right: 16px;
    .detail-content-wrap { padding-right: 0; }
    .content-wrap :deep(.el-form-item__label) { width: min(200px, 35%); }
  }
}

@media (max-width: 760px) {
  .detail-wrap {
    height: auto;
    border-left: 1px solid #e8e8f0;
    padding: 12px;
    .detail-content-wrap { height: auto; overflow: visible; }
    .content-wrap {
      overflow: visible;
      :deep(.el-form-item) { display: block; }
      :deep(.el-form-item__label) {
        width: auto;
        min-height: 20px;
        height: auto;
        line-height: 20px;
      }
    }
    .footer-btn { height: auto; }
  }
}
</style>

<style lang="less">
</style>
