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
const props = defineProps({
  loadingText: {
    type: String,
    default: 'Loading',
  },
  fullscreen: {
    type: Boolean,
    default: false,
  },
})
</script>

<template>
  <div class="u-loading" :class="{ fullscreen: props.fullscreen }" role="status">
    <span class="u-loading-spinner" aria-hidden="true" />
    <span>{{ props.loadingText }}</span>
  </div>
</template>

<style lang="less" scoped>
.u-loading {
  position: absolute;
  inset: 0;
  z-index: 999;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 8px;
  background: rgba(255, 255, 255, .88);
  background: color-mix(in srgb, var(--el-fill-color-blank, #fff) 88%, transparent);
  color: var(--el-text-color-regular);
  font-size: 14px;
  line-height: 20px;
  text-align: center;

  &.fullscreen {
    position: fixed;
  }

  &-spinner {
    box-sizing: border-box;
    width: 24px;
    height: 24px;
    border: 2px solid var(--el-color-primary-light-8);
    border-top-color: var(--el-color-primary);
    border-radius: 50%;
    animation: loading-spin .8s linear infinite;

    @media (prefers-reduced-motion: reduce) {
      animation: none;
    }
  }
}

@keyframes loading-spin {
  to { transform: rotate(360deg); }
}
</style>
