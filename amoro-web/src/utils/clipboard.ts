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

// Clipboard.writeText is only available in secure contexts. Keep the copy fallback
// for existing HTTP deployments and test its return value before reporting success.
// https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText
export async function copyText(text: string): Promise<void> {
  try {
    if (navigator.clipboard?.writeText) {
      await navigator.clipboard.writeText(text)
      return
    }
  }
  catch {
    // Some browsers deny Clipboard API access but allow a user-initiated copy.
  }

  const active = document.activeElement
  const input = active instanceof HTMLInputElement || active instanceof HTMLTextAreaElement ? active : null
  const inputSelection = input ? [input.selectionStart, input.selectionEnd, input.selectionDirection] as const : null
  const selection = window.getSelection()
  const ranges = selection
    ? Array.from({ length: selection.rangeCount }, (_, index) => selection.getRangeAt(index).cloneRange())
    : []
  const field = document.createElement('textarea')
  field.value = text
  field.readOnly = true
  field.tabIndex = -1
  field.style.cssText = 'position:fixed;top:0;left:0;opacity:0;pointer-events:none'
  const container = active?.closest('[role="dialog"]') ?? document.body
  container.append(field)
  try {
    field.select()
    if (!document.execCommand('copy'))
      throw new Error('Copy was denied by the browser')
  }
  finally {
    field.remove()
    if (active instanceof HTMLElement)
      active.focus({ preventScroll: true })
    if (inputSelection?.[0] != null && inputSelection[1] != null)
      input?.setSelectionRange(inputSelection[0], inputSelection[1], inputSelection[2] ?? undefined)
    if (selection && ranges.length) {
      selection.removeAllRanges()
      ranges.forEach(range => selection.addRange(range))
    }
  }
}
