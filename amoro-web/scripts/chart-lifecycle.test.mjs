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

import assert from 'node:assert/strict'
import { readFileSync } from 'node:fs'
// eslint-disable-next-line test/no-import-node-test -- The Dashboard build uses the Node test runner.
import test from 'node:test'
import { SourceTextModule, SyntheticModule, createContext } from 'node:vm'
import ts from 'typescript'
import * as vue from 'vue'
import { parse } from 'vue/compiler-sfc'

const source = readFileSync(new URL('../src/components/echarts/Chart.vue', import.meta.url), 'utf8')
const { descriptor } = parse(source)
const { outputText } = ts.transpileModule(descriptor.script.content, {
  compilerOptions: { module: ts.ModuleKind.ESNext, target: ts.ScriptTarget.ESNext },
})

async function mountChart() {
  const host = { clientWidth: 425, clientHeight: 300 }
  const canvas = { width: host.clientWidth, height: host.clientHeight, drawn: true, resizeCount: 0 }
  let deliverResize
  const context = createContext({
    window: { addEventListener() {}, removeEventListener() {} },
    ResizeObserver: class {
      constructor(callback) { this.callback = callback }
      observe() { deliverResize = this.callback }
      disconnect() { deliverResize = undefined }
    },
  })
  const echarts = {
    init() {
      return {
        setOption() {},
        getWidth: () => canvas.width,
        getHeight: () => canvas.height,
        resize() {
          canvas.width = host.clientWidth
          canvas.height = host.clientHeight
          canvas.resizeCount++
        },
        dispose() { canvas.drawn = false },
      }
    },
  }
  const vueModule = new SyntheticModule(Object.keys(vue), function () {
    for (const [name, value] of Object.entries(vue))
      this.setExport(name, value)
  }, { context })
  const echartsModule = new SyntheticModule(['default'], function () {
    this.setExport('default', echarts)
  }, { context })
  const module = new SourceTextModule(outputText, { context })
  await module.link(specifier => ({ 'vue': vueModule, './index': echartsModule })[specifier])
  await module.evaluate()

  const renderer = vue.createRenderer({
    createElement: () => host,
    insert() {},
    patchProp() {},
    remove() {},
  })
  const app = renderer.createApp({
    ...module.namespace.default,
    render: () => vue.h('div', { ref: 'echart' }),
  })
  app.mount({})
  return {
    app,
    canvas,
    host,
    notifyResize: () => deliverResize?.(),
    isObserving: () => Boolean(deliverResize),
  }
}

test('the canvas follows its container when the viewport is unchanged', async (t) => {
  const { app, canvas, host, notifyResize } = await mountChart()
  t.after(() => app.unmount())
  host.clientWidth = 306
  host.clientHeight = 250
  notifyResize()
  assert.deepEqual([canvas.width, canvas.height], [306, 250])
})

test('unchanged and hidden chart containers do not trigger a redraw', async (t) => {
  const { app, canvas, host, notifyResize } = await mountChart()
  t.after(() => app.unmount())
  notifyResize()
  host.clientWidth = 0
  notifyResize()
  assert.equal(canvas.resizeCount, 0)
})

test('closing a chart releases the canvas and stops observing its container', async () => {
  const { app, canvas, isObserving } = await mountChart()
  app.unmount()
  assert.equal(canvas.drawn, false)
  assert.equal(isObserving(), false)
})
