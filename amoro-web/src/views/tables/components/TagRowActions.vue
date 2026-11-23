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
import { Clock, CopyDocument, Delete } from '@element-plus/icons-vue'

defineProps<{ canCopy?: boolean, canAdjust?: boolean, canDelete?: boolean, observed: boolean, canAdjustRetention: boolean }>()
defineEmits<{ copy: [], retention: [], remove: [] }>()
</script>

<template>
  <el-space wrap>
    <el-tooltip v-if="canCopy" :content="$t('tagCopy')" :trigger="['hover', 'focus']">
      <el-button circle :aria-label="$t('tagCopy')" :disabled="!observed" @click="$emit('copy')">
        <CopyDocument class="amoro-icon" />
      </el-button>
    </el-tooltip>
    <el-tooltip v-if="canAdjust" :content="$t('tagUpdateRetention')" :trigger="['hover', 'focus']">
      <el-button circle :aria-label="$t('tagUpdateRetention')" :disabled="!canAdjustRetention" @click="$emit('retention')">
        <Clock class="amoro-icon" />
      </el-button>
    </el-tooltip>
    <el-tooltip v-if="canDelete" :content="$t('tagDelete')" :trigger="['hover', 'focus']">
      <el-button circle type="danger" :aria-label="$t('tagDelete')" :disabled="!observed" @click="$emit('remove')">
        <Delete class="amoro-icon" />
      </el-button>
    </el-tooltip>
  </el-space>
</template>
