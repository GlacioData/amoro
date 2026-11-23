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
import { computed, ref, watch } from 'vue'
import { useMediaQuery } from '@/hooks/useMediaQuery'

interface PaginationState {
  current: number
  pageSize: number
  total: number
  pageSizeOptions?: readonly (string | number)[]
  showSizeChanger?: boolean
  showQuickJumper?: boolean
  hideOnSinglePage?: boolean
  showTotal?: (total: number) => string
}

const props = defineProps<{ pagination: PaginationState, disabled?: boolean, small?: boolean }>()
const emit = defineEmits<{ change: [value: { current: number, pageSize: number }] }>()
const compact = useMediaQuery('(max-width: 760px)')
const currentPage = ref(props.pagination.current)
const pageSize = ref(props.pagination.pageSize)
watch(() => props.pagination.current, (value) => {
  currentPage.value = value
})
watch(() => props.pagination.pageSize, (value) => {
  pageSize.value = value
})

const layout = computed(() => [
  props.pagination.showTotal && 'slot',
  props.pagination.showSizeChanger && 'sizes',
  'prev',
  'pager',
  'next',
  props.pagination.showQuickJumper && props.pagination.total > pageSize.value && 'jumper',
].filter(Boolean).join(', '))

// One change event yields one request, even when changing size also resets the page.
// https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/pagination.md
function change(current: number, pageSize: number) {
  if (current !== props.pagination.current || pageSize !== props.pagination.pageSize)
    emit('change', { current, pageSize })
}
</script>

<template>
  <el-pagination
    v-model:current-page="currentPage"
    v-model:page-size="pageSize"
    class="dashboard-pagination"
    :page-sizes="pagination.pageSizeOptions?.map(Number)"
    :total="pagination.total"
    :layout="layout"
    :pager-count="compact ? 5 : 7"
    :size="small ? 'small' : 'default'"
    :disabled="disabled"
    :hide-on-single-page="pagination.hideOnSinglePage"
    @change="change"
  >
    <span v-if="pagination.showTotal" class="pagination-total">{{ pagination.showTotal(pagination.total) }}</span>
  </el-pagination>
</template>

<style scoped>
.dashboard-pagination {
  justify-content: flex-end;
  margin-block: 12px;
  flex-wrap: wrap;
  row-gap: 8px;
  white-space: normal;
}
@media (max-width: 760px) {
  .dashboard-pagination {
    --el-pagination-item-gap: 0px;
    display: grid;
    grid-template-columns: max-content minmax(0, max-content) max-content;
    justify-content: start;
  }
  .pagination-total,
  .dashboard-pagination :deep(.el-pagination__sizes),
  .dashboard-pagination :deep(.el-pagination__jump) { grid-column: 1 / -1; }
  .dashboard-pagination :deep(.el-pager li) {
    flex: 1 1 var(--el-pagination-button-width);
    min-width: 0;
  }
}
</style>
