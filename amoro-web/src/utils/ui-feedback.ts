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

import { ElMessage } from 'element-plus'
import 'element-plus/es/components/message/style/css'
import i18n from '@/language/i18n'

export const message = ElMessage

interface Confirmation {
  title: string
  content?: string
  okText?: string
  onOk?: () => unknown
  onCancel?: () => void
}

function confirmationButtons(okText?: string) {
  return {
    confirmButtonText: okText ?? i18n.global.t('ok'),
    cancelButtonText: i18n.global.t('cancel'),
  }
}

async function messageBox() {
  const [{ ElMessageBox }] = await Promise.all([
    import('element-plus/es/components/message-box/index'),
    import('element-plus/es/components/message-box/style/css'),
  ])
  return ElMessageBox
}

// The confirmation stays open while its handler saves. Cancel and close never call onOk.
// https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/message-box.md
export function confirmAction({ title, content = '', okText, onOk, onCancel }: Confirmation): void {
  let settled = false
  void messageBox().then(box => box.confirm(content, title, {
    ...confirmationButtons(okText),
    type: 'warning',
    closeOnClickModal: false,
    beforeClose: async (action, instance, done) => {
      if (instance.confirmButtonLoading)
        return
      if (action !== 'confirm') {
        settled = true
        onCancel?.()
        done()
        return
      }
      instance.confirmButtonLoading = true
      try {
        await onOk?.()
        settled = true
        done()
      }
      catch {
        // Request/validation feedback belongs to the caller. Keep the action retryable.
      }
      finally {
        instance.confirmButtonLoading = false
      }
    },
  })).catch(() => {
    if (!settled)
      onCancel?.()
  })
}

export function warnAction({ title, content = '', okText }: Confirmation): void {
  void messageBox().then(box => box.alert(content, title, {
    ...confirmationButtons(okText),
    type: 'warning',
  })).catch(() => {})
}
