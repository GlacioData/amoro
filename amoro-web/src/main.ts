/*
  * Licensed to the Apache Software Foundation (ASF) under one
  * or more contributor license agreements.  See the NOTICE file
  * distributed with this work for additional information
  * regarding copyright ownership.  The ASF licenses this file
  * to you under the Apache License, Version 2.0 (the
  * "License"); you may not use this file except in compliance
  * with the License.  You may obtain a copy of the License at
  *
  *     http://www.apache.org/licenses/LICENSE-2.0
  *
  * Unless required by applicable law or agreed to in writing, software
  * distributed under the License is distributed on an "AS IS" BASIS,
  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  * See the License for the specific language governing permissions and
  * limitations under the License.
  */

import { createApp } from 'vue'
import { createPinia } from 'pinia'

import App from './App.vue'
import router from './router'
import useStore from './store'
import VueI18n from './language/i18n'

import RegisterComponents from './components/register'

import './styles/index.less'
import loginService from './services/login.service'
import { getQueryString } from './utils'
import { resetLoginTip } from './utils/request'
import {
  canAccessRoute,
  capturePreLoginTarget,
  getDefaultRoute,
  normalizeRoutePath,
} from './utils/permission'
import SvgIcon from '@/components/svg-icon.vue'

const pinia = createPinia()
const app = createApp(App).use(pinia)
app.component('svg-icon', SvgIcon)
app.use(VueI18n)
RegisterComponents(app);

// login
(async () => {
  try {
    const store = useStore()
    const token = getQueryString('token') || ''
    const res = await loginService.getCurUserInfo(token)
    if (res) {
      const roles = res.roles || (res.role ? [res.role] : [])
      const privileges = res.privileges || []
      store.updateUserInfo({
        userName: res.userName,
        role: roles[0],
        roles,
        privileges,
      })
    }
  }
  finally {
    const store = useStore()
    router.beforeEach((to, from) => {
      const isAuthenticated = Boolean(store.userInfo.userName)
      const preLoginTarget = capturePreLoginTarget(to, from, isAuthenticated)
      if (preLoginTarget) {
        store.setHistoryPath(preLoginTarget)
      }
      if (normalizeRoutePath(to.path) === '/login') {
        resetLoginTip()
        return true
      }
      if (!isAuthenticated) {
        return {
          path: '/login',
        }
      }
      if (!canAccessRoute(to.path)) {
        return {
          path: getDefaultRoute(),
        }
      }
      return true
    })

    app.use(router)
    app.mount('#app')
  }
})()
