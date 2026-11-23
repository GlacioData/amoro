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
import { ArrowDown, ChatDotRound, QuestionFilled, SwitchButton } from '@element-plus/icons-vue'
import { onMounted, reactive } from 'vue'
import { useI18n } from 'vue-i18n'
import { useRouter } from 'vue-router'
import { confirmAction } from '@/utils/ui-feedback'

import useStore from '@/store'
import { getVersionInfo } from '@/services/global.service'
import loginService from '@/services/login.service'

interface IVersion {
  version: string
  commitTime: string
}

const verInfo = reactive<IVersion>({
  version: '',
  commitTime: '',
})

const { t, locale } = useI18n()
const router = useRouter()
const store = useStore()
function roleText() {
  return (store.userInfo.roles || []).join(', ') || '-'
}

async function getVersion() {
  const res = await getVersionInfo()
  if (res) {
    verInfo.version = res.version
    verInfo.commitTime = res.commitTime
  }
}

function goLoginPage() {
  router.push({ path: '/login' })
}

async function handleLogout() {
  confirmAction({
    title: t('logoutModalTitle'),
    onOk: async () => {
      try {
        await loginService.logout()
      }
      catch (error) {
      }
      finally {
        store.updateUserInfo({
          userName: '',
          roles: [],
          privileges: [],
        })
        goLoginPage()
      }
    },
  })
}

function goDocs() {
  window.open('https://amoro.apache.org/docs/latest/')
}

function setLocale() {
  if (locale.value === 'zh') {
    locale.value = 'en'
  }
  else {
    locale.value = 'zh'
  }
}

onMounted(() => {
  getVersion()
})
</script>

<template>
  <div class="custom-top-bar">
    <div class="version-info" :title="`${$t('version')}: ${verInfo.version} · ${$t('commitTime')}: ${verInfo.commitTime}`">
      <span class="g-mr-8">{{ `${$t('version')}:  ${verInfo.version}` }}</span>
      <span class="g-mr-8 version-commit-time">{{ `${$t('commitTime')}:  ${verInfo.commitTime}` }}</span>
    </div>
    <el-dropdown :trigger="['hover', 'click']">
      <button type="button" class="account-trigger">
        <span class="role-badge">{{ roleText() }}</span>
        <span class="account-name">{{ store.userInfo.userName }}</span>
        <ArrowDown class="amoro-icon" />
      </button>
      <template #dropdown>
        <el-dropdown-menu>
          <el-dropdown-item key="userGuide" @click="goDocs">
            <QuestionFilled class="amoro-icon" /> {{ $t('userGuide') }}
          </el-dropdown-item>
          <el-dropdown-item key="locale" @click="setLocale">
            <ChatDotRound class="amoro-icon" /> {{ locale === 'zh' ? '切换至英文' : 'Switch To Chinese' }}
          </el-dropdown-item>
          <el-dropdown-item key="logout" @click="handleLogout">
            <SwitchButton class="amoro-icon" /> {{ $t('logout') }}
          </el-dropdown-item>
        </el-dropdown-menu>
      </template>
    </el-dropdown>
  </div>
</template>

<style lang="less">
  .custom-top-bar {
    height: 48px;
    display: flex;
    flex: 0 0 48px;
    min-width: 0;
    align-items: center;
    justify-content: flex-end;
    background: #fff;
    border-bottom: 1px solid #e8e8e8;
    box-shadow: 0 1px 2px rgb(0 21 41 / 5%);
    padding: 0 12px 0 0;
    font-size: 12px;
    gap: 8px;

    .version-info {
      min-width: 0;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    .account-trigger {
      display: inline-flex;
      align-items: center;
      gap: 4px;
      border: 0;
      min-height: 36px;
      background: transparent;
      color: inherit;
      font: inherit;
      cursor: pointer;
    }

    .account-name {
      max-width: 10ch;
      overflow: hidden;
      text-overflow: ellipsis;
      white-space: nowrap;
    }

    @media (max-width: 760px) {
      padding-inline: 8px;
      .version-commit-time { display: none; }
      .role-badge { margin: 0; font-size: 10px; }
    }
    .question-icon {
      font-size: 12px;
      margin-top: -2px;
    }
  }
  .topbar-tooltip .el-popper {
    font-size: 12px;
  }
  .logout-button.el-button {
    border: none;
  }
  .logout-button:hover {
    border-color: unset;
  }
  .role-badge {
    display: inline-block;
    margin-right: 8px;
    padding: 1px 6px;
    border-radius: 10px;
    background: #f0f5ff;
    color: #1d39c4;
  }
</style>
