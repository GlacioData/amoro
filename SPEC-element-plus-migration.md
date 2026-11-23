<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements. See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership. The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License. You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied. See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# Spec：amoro-web 迁移至 Element Plus 2.14.8

- 状态：迁移代码已实施，Chrome 本地回归和两项主性能目标已通过。详见 [Plan](tasks/plan.md) 和 [任务清单](tasks/todo.md)。
- 更新：2026-11-20，Asia/Singapore。
- 分支：`codex/element-plus-migration`。
- 工作目录：`/Users/SL/.codex/worktrees/element-plus-migration/amoro`。
- 本次实施已将迁移分支 rebase 到远端 `sl/dev` 的 `591b90ca5875d748eb0fd9ddcea481112781f76c`，以该提交作为使用旧 UI 的比较基线 B0。
- T01 对齐实施时的 `sl/dev` 并冻结比较基线 B0。旧 UI 与迁移版均使用 B0 的后端源码及等价配置。

## 1. 目标与用户已确认的约束

面向现有 AMS Dashboard 用户和前端维护者，将 Ant Design Vue 及其相关旧配件替换为 Element Plus `2.14.8` 与可维护的实现，减少不必要的前端代码解析和渲染开销。

**最高优先级：不得改变当前任何业务功能。** 原有可用功能、数据含义、权限、接口、用户操作结果和错误处理契约必须保持。性能提升不能以删功能、减少可见信息、降低数据完整性、放宽权限或缩减验证覆盖实现。

本轮执行以下约定：

1. 本次版本复核将 Element Plus 从原目标 `2.14.7` 上调到 **`2.14.8`**，图标库精确锁定 **`2.3.2`**。
2. **保留页面布局、信息层级、操作位置和现有交互，允许基础控件采用 Element Plus 风格。** 不要求逐像素复刻 Ant Design；不允许借迁移重新设计页面或改变工作流。
3. 同机器、同数据、同浏览器重复测量，**首屏 JavaScript 解析/编译 CPU 耗时、预先选定的核心页面渲染耗时，中位数各降低至少 10%**；其他关键操作无可复现退化。
4. 继承前一阶段的交付要求：完成替换后必须本地启动真实 Amoro，模拟用户点击、输入、滚动和键盘操作，检查功能、组件格式及性能。

版本兼容声明、官方文档和源码核查只是实施依据，不能证明迁移后已经正确或已经更快。本 Spec 的性能数字是验收目标，尚无本轮实测结果。

## 2. 现状与事实依据

### 2.1 当前前端基础

按本次 `sl/dev` 的 `amoro-web/pnpm-lock.yaml`，现有实际锁定版本为 Vue `3.5.44`、Router `5.4.0`、Pinia `4.1.0`、Vue I18n `11.4.14`、Axios `1.20.0`、Day.js `1.11.23`、ECharts `6.1.0`、Swagger UI `5.33.2`、`vue-virtual-scroller@3.0.5`。

工具链锁定 Vite `8.3.4`、TypeScript `6.0.3`、vue-tsc `3.3.13`、`unplugin-vue-components@32.1.0`，Maven 固定 Node `22.23.2` 和 pnpm `9.6.0`。npm 已有更新的 Vue、Router、Vue I18n、Axios、Swagger UI、Vite、TypeScript 和 vue-tsc。它们按 B0 锁定版本参与此次对照，避免同时改变基础框架、工具链和 UI 库而无法归因。ECharts 和现有虚拟滚动也按 B0 锁定版本参与对照。

Element Plus `2.14.8` 于 2026-11-06 发布，是本次核查时的 npm 稳定版。它要求 Vue `^3.3.7`，依赖 `@element-plus/icons-vue@^2.3.2` 与 `@vueuse/core@15.0.0`。图标库当前稳定版为 `2.3.2`，要求 Vue `^3.2.0`。VueUse 15 要求 Vue `^3.5.0` 和 Node `>=22`。本次核查的 `sl/dev` 锁定版本满足这些声明。发布包已核对按需 CSS 和中英文 locale 入口，实际安装、类型检查、构建与运行由 T08、T46 和 T47 验证。[2.14.8 发布说明](https://github.com/element-plus/element-plus/releases/tag/2.14.8) · [发布包元数据](https://registry.npmjs.org/element-plus/2.14.8) · [图标发布包](https://registry.npmjs.org/@element-plus/icons-vue/2.3.2) · [VueUse 15 元数据](https://registry.npmjs.org/@vueuse/core/15.0.0)

新版将 `@ctrl/tinycolor` 的声明范围由 `^4.2.1` 调整为 `^4.2.2`。T08 记录其实际解析版本，T45 核对分发许可。依赖范围以发布包为准，具体版本以生成的锁文件为准。[原目标元数据](https://registry.npmjs.org/element-plus/2.14.7) · [新目标元数据](https://registry.npmjs.org/element-plus/2.14.8)

### 2.2 迁移规模与耦合

基线源码有 **60 个 `.vue` 文件**；按模板中小写 `<a-…>` 开始标签统计，有 **39 类、368 处**引用，其中 Table 28、Form 12、FormItem 51、Modal 5。该统计不包含 `AModal`、`ATooltip`、`AProgress` 等别名引用、图标和命令式调用，不能作为全部迁移点清单。

- `App.vue`：Ant Design 的中英文 locale 与 Day.js 联动。
- `main.ts`、`vite.config.mts`：全局 SVG 注册、自动组件/图标解析、`antd` vendor 分组。
- `utils/request.ts`、多个页面：命令式 `message`、`Modal.confirm` 等反馈。
- `views/catalogs/Detail.vue`：上传组件、文件状态/类型、请求头与业务响应处理。
- `views/tables/components/Details.vue` 等：Ant Design 表格类型、列定义和 slot。
- `styles/ant-design.less`、页面 `:deep(.ant-…)`：全局覆盖、布局和组件内部类名耦合。
- `views/tables/components/Operations.vue`：`vue-clipboard3` 复制操作详情。
- `src/assets/icons/svg/`：16 个本地 SVG，包括 Amoro、Iceberg、Hive、Paimon 及菜单图标；`SvgIcon` 提供动态名称、颜色、hover、disabled 和 click 行为。
- `VirtualRecycleScroller.vue`：仍维护的虚拟滚动组件与 Ant Design Empty 存在组合依赖。

实施前须补齐逐文件清单，覆盖模板、别名、TS 类型、JS 调用、样式和测试选择器。不能将成功替换 39 个标签名当作功能迁移完成。

### 2.3 维护状态的表述边界

本次 npm 核查中，四个待替换包的 `latest` 都与 `sl/dev` 锁定版本一致，分别为 Ant Design Vue `4.2.6`、Ant Design Vue 图标 `7.0.1`、`vue-clipboard3@2.0.0` 和 `vite-plugin-svg-icons@2.0.1`。四者的 npm 发布记录均未标记 deprecated。替换范围依据已确认的选型和维护成本。详见本页末尾的发布记录。

## 3. 替换范围与明确方向

| 当前实现 | 本次目标 | 必须保持/核查 |
|---|---|---|
| `ant-design-vue@4.2.6` | **`element-plus@2.14.8`，直接声明精确版本** | 全部 UI、TS 类型、命令式 API、locale、样式和依赖图清理 |
| `@ant-design/icons-vue@7.0.1` 及自动图标解析 | `@element-plus/icons-vue@2.3.2`，按实际使用显式导入 | 图标含义、可辨识性、点击区域、loading、disabled、tooltip 和可访问名称；无等价图标时保留受控本地 SVG |
| `vue-clipboard3@2.0.0` | 平台 Clipboard API 与本地小型兼容封装，不额外引入剪贴板包 | 真实剪贴板内容及成功/失败语义；保留普通 HTTP 下基线已有的复制能力，见第 5.4 节 |
| `vite-plugin-svg-icons@2.0.1`、虚拟注册入口、专属类型与 packageExtension | 同源 SVG sprite 与 Vite 静态资源导入 | 保留全部现有品牌/格式素材及 `SvgIcon` 使用契约；删除插件后再移除其 `fast-glob` 供给补丁 |
| `AntDesignVueResolver` | 现有 `unplugin-vue-components` 的 `ElementPlusResolver` | 组件及样式按需解析，必要的指令解析；命令式 API 的样式单独核对 |
| Ant Design 样式覆盖、`antd` chunk 规则 | Element Plus 按需 CSS、项目主题变量与必要的布局适配 | 布局/密度/可读性、浮层层级、静态资源路径、异步 chunk 恢复 |

保留 Vue、Router、Pinia、Vue I18n、Axios、Day.js、ECharts、Swagger UI、`vue-virtual-scroller` 和 devtools peer 关系；不得为“统一技术栈”额外替换这些实现。ECharts 的主题、图表数据、交互与现有按需注册保持。

最终直接声明、锁文件及分发模块中均应移除四个旧包：`ant-design-vue`、`@ant-design/icons-vue`、`vue-clipboard3`、`vite-plugin-svg-icons`。若发现意外传递引用，必须定位并解决或先调整 Spec，不能仅删除 `package.json` 条目后宣布完成。只删除由实际依赖图证明已无用途的附属包、样式及许可条目。

本次不新增页面、功能、设置项、埋点或暗色模式；不恢复已删除的 Terminal；不改 Java 业务、数据库、接口协议、后端分页或权限模型；不以调整构建工具主版本作为迁移前提。

## 4. 不得改变的功能契约

“不改变功能”须通过操作与数据比对验证，不依赖实现者主观判断。实施前冻结一份功能对照表：功能 ID、入口、适用权限/格式/能力、操作步骤、请求契约、可见结果、刷新后的状态、基线证据、迁移版证据。以下为最低覆盖范围，不是删除未列功能的许可。

| 功能域 | 必须保持的行为 |
|---|---|
| 登录、身份与权限 | 当前用户初始化、登录/退出、登录失效提示、token 入口、角色/privilege 判断、默认可访问路由、按钮和菜单隐藏/禁用；不得扩大可操作权限 |
| 路由、导航与状态 | 原路由及 query、hash history、旧 Hive 路径重定向、404、Logo/侧栏导航、浏览器前进后退、刷新、选中状态及原有 sessionStorage 状态；不得改变 push/replace 语义 |
| Catalog 与数据库 | Catalog 列表、详情/新建/编辑/删除、类型/存储/认证/格式选项、属性编辑、上传与下载、状态检查、数据库选择/创建；表单显示条件与保存内容一致 |
| Table Explorer 与表入口 | catalog/database/table 分层、懒加载、刷新、单词与层级搜索、既有防抖、展开/选中恢复、虚拟滚动、空库/空结果、切换时的请求失效处理；表创建及 Hive 升级流程不变 |
| 表详情与健康信息 | Details、字段/分区/属性、Table Model、Paimon 健康分数/结构/对比及弹窗；可见信息、数值精度、单位、未知/空值呈现和可用操作一致 |
| Snapshots、Files | 当前有界/旧版能力分支、snapshot/branch/tag 选择及定位、分页/列/展开、面包屑、文件统计、图表/tooltip、加载和限流/失效处理；不得漏数据或改变排序/时间含义 |
| Tags | 名称搜索及 query、分页和 page size、创建、复制及二次确认、保留期模式/小时数、删除时名称确认、快照跳转；格式能力、只读、观察 token、确认过期、失败结果不确定语义均保持 |
| Optimizing / Process / Operations | 原有列表、筛选/重置、分页、详情、优化操作、进程信息、操作详情弹窗和完整文本复制；成功后刷新、失败提示、取消行为一致 |
| Optimizer / 资源管理 | Optimizing 路由当前指向的 Resource 页面及其三个标签页、资源组创建/编辑/删除、Optimizer 创建/扩缩容、筛选/分页、参数校验和确认；按既有入口与权限保持 |
| Overview 与 Settings | 所有统计卡、Top10、时间范围、图表 legend/tooltip/resize、系统配置、容器折叠及当前可用设置操作；不得减少请求结果或信息卡来提速 |
| Swagger 与公共反馈 | `/openapi-ui`、`/swagger-docs`、接口分组/schema/CSS；消息、错误、确认、空态、loading、重试与页面恢复保持可用 |
| 中英文与输入 | 用户文案及动态语言切换、组件内置文案、数字/时间展示、键盘输入/Enter/Tab/Escape、中文输入法组合输入、焦点与可访问名称；不改变当前时区/单位/精度 |

上述只读和写操作均须覆盖成功、取消、校验失败、请求失败、权限/能力不支持、loading 等适用状态。没有权限或数据时记录“待验收”，不能删除对应功能或用“未见异常”代替证据。

## 5. 组件迁移的兼容要求

### 5.1 组件族映射

映射描述必须达到的等价能力，不把 API 名称相似当作可直接替换。

| 当前组件/用法 | 目标组件/机制 | 重点验收 |
|---|---|---|
| Button、Input、InputSearch、InputNumber、Select/Option、AutoComplete、Checkbox/Group | Element Plus 对应控件；搜索框由 Input 与现有搜索触发组合 | `v-model:value` 到目标 model 的值类型；清空值、单/多选、数值边界、精度、禁用、搜索触发时机/次数、防抖、Enter 与中文输入法 |
| Form / FormItem | Form / FormItem | rules、prop 路径、必填/自定义/异步校验、触发时机、重置/清除错误、校验后的保存流程；不得新增或放宽业务约束 |
| Table（28 处）、Pagination | **标准 Table / TableColumn + Pagination 为默认方向** | 列与 slot、row key、行点击、展开、固定列/表头、空值/空态、loading、横纵滚动、ellipsis/tooltip、分页索引、page size、total；一次操作不能触发重复请求 |
| Tree 与已有 RecycleScroller | Tree 与原有虚拟列表；按现有能力做渲染优化 | 懒加载/搜索/展开/选中/滚动位置及键盘行为；不得一次性加载所有后端表或去掉已有虚拟化 |
| Modal / 命令式 Modal | Dialog / MessageBox，复杂表单使用 Dialog | 显隐 model、footer、取消/关闭/确认区分、mask/Escape、focus、销毁/保留表单状态、异步提交、防重复操作及卸载清理 |
| message / Alert / Spin / ULoading | Message / Alert / Loading 及必要的薄封装 | 提示类型/内容/时机、登录失效提示去重、遮罩范围、loading 文案与阻塞范围、关闭/卸载后资源释放 |
| Menu、Dropdown、Tabs、Breadcrumb、Collapse | 对应导航组件 | 活动项、路由触发次数、展开/折叠状态、disabled、键盘操作、tab 切换后的数据/状态与原有挂载行为 |
| Upload | Upload 及现有上传业务适配 | 地址、multipart 字段、`X-Request-Source`/鉴权、文件类型/数量、回显/删除/重传/下载、取消、HTTP 与业务状态分别处理 |
| Row/Col、Space、Card、Descriptions、Divider、Statistic、TypographyText、Progress、Empty、Tooltip | 对应布局/展示组件或语义等价 HTML | 网格、间距、格式化/精度、可见文本、进度单位、截断、hover/focus 浮层；Typography 复制等实际使用能力不得遗漏 |
| ConfigProvider、locale | ConfigProvider + 中英文 locale，保留 Vue I18n / Day.js 联动 | 动态切换、浮层与命令式实例文案、日期/数字语义、样式作用域 |

Element Plus `2.14.8` 的 Table V2 官方文档仍标为 beta。默认使用标准 Table、Tree 和 Select。具体场景采用虚拟化实现前，须用 trace 定位瓶颈并验证功能等价与性能收益。[固定版本 Table V2 文档](https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/table-v2.md)

将 `2.14.8` 涉及本项目的修复纳入回归用例。侧栏用原有间距在两种视口检查折叠和选中时的抖动。Catalog 表单与 Tag 保留期对话框检查动态禁用时的 Select 候选、焦点和提交行为，以及 InputNumber 的数值与 ARIA。Operations 和 Tag 对话框检查自定义标题的无障碍关联。表格检查实际动态列切换和表头插槽是否随列同步更新。[2.14.8 发布说明](https://github.com/element-plus/element-plus/releases/tag/2.14.8)

### 5.2 异步、错误与数据不变量

- 表单提交以既有业务状态为准；Tag 等关键写操作在 pending 时仍阻止重复点击/双 Enter/错误关闭，loading 结束时机不提前。
- MessageBox 取消/关闭产生的 Promise rejection 必须按预期分支处理；不能出现新增未捕获异常，也不能将真实失败吞掉或显示成功。
- 上传 HTTP 200 不代表业务成功；非法 XML、失败文件 ID 和上传中状态不得被误绑定为成功配置。保留既有可选/必填条件与合法保存结果。
- 页面切换、筛选和快速选择时仍丢弃过期响应；不得因优化缓存跨表、跨 catalog、跨用户复用错误数据。保持现有刷新与失效契约。
- 请求 endpoint、method、query/body 字段、鉴权与业务请求头、分页语义、成功/错误判断及写入结果一致。允许减少有证据的纯 UI 重复计算；不得删减必要请求或改变后端契约来美化数据。
- 不把大型 snapshot ID 当作普通 Number，不改变字符串/布尔值/`undefined`/`null`/空字符串、日期时区、字节单位、百分比与精度；包括控件默认清空行为引起的隐式变化。

### 5.3 样式、资源与解析开销

- 使用现有 `unplugin-vue-components` 的 `ElementPlusResolver` 做组件与 CSS 按需引入；命令式 Message、MessageBox、Loading 等必须显式核查其样式加载。采用预编译 CSS 与主题 CSS 变量，继续兼容项目 Less；不为基础主题新增 Sass 工具链。
- 不全量 `app.use(ElementPlus)`，不全量注册图标，不导入所有 locale，不以全量 CSS/JS 掩盖漏样式或解析器问题。不得引入第二份 Vue/Day.js 或并存两套 UI 作为最终交付。
- 保留 `base: './'`、AMS 静态资源目录、route lazy import 与 preload 恢复；重新核查 Vite 8/Rolldown 分组对按需加载的真实影响，不机械将 `antd` 分组名改为 `element-plus` 后宣布已优化。
- 替换 `.ant-*` 内部类依赖；业务类允许保留或改名，但不能为了删除字符串而做无关重构。检查全局 reset、Less 与 Element Plus CSS 的加载顺序，避免影响 ECharts、Swagger、暗色侧栏等现有区域。
- 所有资源由本地构建产物供应，不增加运行时 CDN 依赖。原有 Logo 和表格式 SVG 不替换成含义近似的通用图标；原有 hover/stroke/disabled/click 与尺寸行为保持。
- SVG 只处理仓库内已审查的静态素材，不将用户输入、接口字段或远程 SVG 作为原始 HTML 注入。动态名称解析、重复 ID、viewBox、`currentColor`、引用兼容性均须验证。

### 5.4 剪贴板兼容边界

原生 Clipboard API 受安全上下文与浏览器权限约束，普通远程 HTTP 不能假设与 localhost 等价。替换须保留基线支持的复制场景：优先平台 Clipboard API；在不支持/拒绝时使用受控本地兼容路径，并恢复焦点、选区和临时节点。

如兼容路径需要 `document.execCommand('copy')`，只将其用于保持既有 HTTP 能力，检查其返回值/异常；不能将已废弃 API 当成新的通用依赖方向，不能在复制失败时显示成功。必须实际比对剪贴板文本，包含换行、引号、Unicode 和长操作文本。不得通过要求所有既有部署改为 HTTPS 来绕过功能兼容问题。[Clipboard API 约束](https://developer.mozilla.org/en-US/docs/Web/API/Clipboard/writeText)、[execCommand 状态与兼容边界](https://developer.mozilla.org/en-US/docs/Web/API/Document/execCommand)。

## 6. 性能验收合同

### 6.1 固定工作负载与测量口径

迁移前先用第 2 节基线构建真实 AMS，冻结工作负载、数据规模、测量起止和脚本；迁移版按同一口径重放，不能看到结果后挑选更有利的页面。上阶段的历史报告仅供背景参考，不能直接充当本次比较基线。

| 指标 | 固定场景与口径 | 交付门槛 |
|---|---|---|
| P1：首屏 JS 解析/编译 CPU | 有效登录态下冷启动硬导航至 `/#/tables`，至首批树/表数据可操作；同一 trace 口径汇总主线程 JS parse/compile 的独占 CPU 时间，避免嵌套事件重复计数，另报 evaluate/script 总时间 | 解析/编译 CPU 中位数较基线降低 **≥10%**；执行 CPU 不能出现可复现退化 |
| P2：核心页面渲染耗时 | 同一非空测试表，从点击 Snapshots tab 到首批快照/统计可见且可操作；另标记最后必要响应到首个完成绘制帧的前端渲染区间。固定数据量、页大小、展开状态和控件显示 | 后一前端渲染区间的中位数较基线降低 **≥10%**；完整点击到可操作时延无可复现退化 |
| P3：其他关键操作 | 登录、侧栏切页、表树展开/搜索、列表分页、创建 Tag 弹窗、确认/取消、表格滚动、图表 resize | p50、p95、长任务、可见卡顿均核查，无迁移引入的可复现退化 |
| P4：初始资源与渲染成本 | 各路由实际加载 JS/CSS 的 raw/gzip/传输量、模块来源；trace 的 script/style/layout/paint、DOM、长任务数量与时长 | 不加载无关 UI/整套图标；新增资源或 CPU 成本必须归因，包体变小不能替代 P1/P2 |
| P5：视觉稳定与重复操作 | 登录首屏 LCP/CLS；页面往返、弹窗、列表/图表重复操作至少 10 轮 | 无新增布局错乱/异常偏移、重复监听/请求/挂载、不可恢复状态或新增泄漏证据 |

P1 的解析/编译与执行分开报告，不把后端响应变快、包体变小或单次 LCP 下降说成解析更快。P2 必须包含组件更新和实际绘制，`nextTick`、固定等待或接口返回时间不能单独作为渲染结束证据。记录真实帧/trace 与可操作断言；动画和测试工具固定等待单独列出。

### 6.2 公平性、噪声与失败处理

- 基线/迁移版使用同机器、同 JDK、同浏览器版本、同测试数据、同语言/视口/网络与 CPU 条件，正式产物，不开 DevTools 扩展或 dev/mock 构建。冷启动清理 HTTP/代码缓存并记录方法，热交互单独测量；浏览器驱动串行，避免互相争用本地 AMS。
- P1、P2 和关键交互每端至少 **10 个有效样本**，交错执行基线与迁移版；保存原始 trace、逐次结果、中位数、p95、离散程度和差异。样本排除规则必须事先固定，不能选择性丢掉慢样本。
- 提升比例为 `(基线中位数 - 迁移版中位数) / 基线中位数`。10% 收益还须超过同基线重复测量噪声；按配对样本估计的差异 95% 置信区间跨 0 时，增加采样并归因，不宣称已提升。基线为零或工具无法测得对应事件时标为待验收，不能换用有利的替代指标。
- P3 任一关键耗时的 p50/p95 上升超过 `max(基线值 × 10%, 16ms)` 时启动归因与复测。此数值仅为调查触发线；任何已确认由迁移引入的退化，包括线下可重现卡顿，均须修复。
- 树/列表覆盖真实 AMS 中的小数据和至少 1,000 个可展示节点/条目的规模，固定 fixture 与预期数量。不能为性能测试绕过已有服务端分页、扩大生产请求、只渲染部分数据而丢失访问能力；超大合成组件压测仅作补充。
- 重复操作内存检查需结合 GC、DOM、监听器/实例和响应资源存活分析；不凭一次 heap 值宣布无泄漏，也不把已知基线增长自动归咎于迁移。
- **功能等价与 P1/P2 两项性能收益必须同时满足。** 未达标时继续定位按需导入、分组、重复计算/挂载和渲染瓶颈；不能仅以“换库完成/包小了/看起来流畅”结项，不能自行降低阈值。若客观无法达到，报告证据并重新审阅 Spec。

## 7. 项目结构与代码风格

| 路径 | 本次职责 |
|---|---|
| `SPEC-element-plus-migration.md` | 需求、功能不变量、性能目标与验收边界；后续 Plan/Tasks 另行批准后编写 |
| `amoro-web/package.json`、`pnpm-lock.yaml` | 精确目标版本、旧包及无用途补丁清理、可复现依赖图 |
| `amoro-web/vite.config.mts`、`src/main.ts`、`src/vite-env.d.ts`、`src/App.vue` | 解析器、CSS、资源、注册入口、locale；`components.d.ts` 按当前忽略策略生成并检查，不强行纳入版本管理 |
| `amoro-web/src/components/`、`src/views/`、`src/styles/` | 全部组件、图标、浮层、表格/表单/上传和主题适配；保留业务行为 |
| `amoro-web/src/utils/`、`hooks/`、`types/` | 必要的反馈/剪贴板/组件类型适配，不重写业务模型和权限判断 |
| `amoro-web/scripts/` | 保留现有导航、preload、表/快照/Tag 状态测试，增加真实迁移风险的行为测试 |
| `amoro-web/licenses/`、`README.md`、必要的前端打包配置 | 更新实际分发许可、技术栈与操作说明，不改后端行为 |
| `output/element-plus-verification/` | 本地基线、trace、截图、命令日志、功能对照与验收报告；不提交数据库、缓存、产物、凭据或运行配置 |

沿用 Vue 3 Composition API、TypeScript、两空格、单引号和当前 ESLint 约定；保留 ASF 文件头。示例取自现有 `Tags.vue` 的业务条件，迁移应保留这些条件，只适配 UI：

```ts
const allowed = (action: string) => canManageTable() && capabilities.value?.actions.includes(action)

async function submit() {
  if (busy.value || !valid.value || (copying.value && !confirming.value))
    return
  // 后续业务校验、写入、结果判断与刷新继续保持原有语义。
}
```

共享封装仅用于已出现的语义差异和复用需求，不构建整套“兼容 Ant Design API”的第二组件库；不得用无依据的 `any`、`@ts-ignore`、关闭 lint/类型检查、空 catch 或删除测试掩盖迁移问题。

## 8. 实施阶段验证命令与测试策略

以下命令供后续实施验收执行，**本次 Spec 阶段尚未执行这些产品验证**。先确认运行的 Node/pnpm 与 Maven 基准一致；如使用 Maven 下载的工具，将其加入当前验收进程 PATH 并记录实际路径，不修改用户的全局配置。

```bash
cd /Users/SL/.codex/worktrees/element-plus-migration/amoro/amoro-web
node --version
pnpm --version
pnpm install --frozen-lockfile --strict-peer-dependencies --config.engine-strict=true
pnpm run typecheck
pnpm exec vue-tsc --noEmit -p tsconfig.node.json
pnpm run lint
pnpm run build
pnpm audit --prod --json
```

保留 `build` 中所有现有测试。针对真实风险新增行为断言，优先沿用现有 `node:test`/脚本与真实浏览器工具；测试关注参数、请求次数、界面结果、错误和卸载行为，不为包名/标签名建立镜像测试。必要时只等价更新 Ant Design DOM 选择器，禁止删减断言通过迁移。

基线 lint 已有诊断，实施前重新冻结逐文件/规则结果；迁移版不得增加未解决诊断，不得因删除组件而静默放弃有效检查。审计须比较直接/传递路径与实际分发内容；不得声称换库自动消除全部历史漏洞。

辅助开发分别验证 `pnpm run dev`、`pnpm run dev:mock`。最终功能和性能验收使用 AMS 正式产物。安装后确认 `element-plus@2.14.8`、`@element-plus/icons-vue@2.3.2` 和新增传递依赖的实际解析版本，并核对重复实例和旧包路径。锁文件由 pnpm 生成。

正式分发验证命令：

```bash
cd /Users/SL/.codex/worktrees/element-plus-migration/amoro
CI=true ./mvnw -pl dist -am package -DskipTests
mkdir -p output/element-plus-verification/runtime
tar -xzf dist/target/apache-amoro-1.0.4-bin.tar.gz -C output/element-plus-verification/runtime
```

使用基线已验证的 JDK 11 环境并记录精确版本。`-DskipTests` 仅用于此分发组装，不应被描述为 Java 测试通过。最终打包前清理本工作区的旧前端输出/`target`，确保 JAR 的 static 与当前 Vite 输出一致，不携带旧哈希资源。核对第三方许可证、版权和 NOTICE；`pnpm run gen:license` 只处理 ASF 源码头，不等同于第三方许可审计。

## 9. 本地 Amoro 与浏览器验收

### 9.1 启动、数据与产物身份

发行命令以本次核查的 Amoro `1.0.4` 为例。T01 冻结 B0 后按实际 Maven 版本和 assembly 调整路径。在各自解压目录的 `conf/config.yaml` 配置独立 Derby、catalog/warehouse、空闲端口和非默认测试管理员密码。正式回归由 AMS 同源提供页面和 API。开发时如使用独立 Vite 源，使用同源代理或在隔离 AMS 中配置精确的 `ams.http-server.cors.allowed-origins`。B0 和迁移版使用同样的 fixture，写入后恢复快照再比较。

```bash
cd /Users/SL/.codex/worktrees/element-plus-migration/amoro
bash output/element-plus-verification/runtime/amoro-1.0.4/bin/ams.sh start-foreground
```

默认地址 `http://127.0.0.1:1630`；隔离端口必须写入报告。也可用 IDE 启动 `org.apache.amoro.server.AmoroServiceContainer.main()`，但必须证明加载的是当前正式前端产物。记录 commit、前端入口/JS/CSS hash、JAR/发行包 hash、JDK/AMS PID、端口、DB/fixture 和浏览器版本；通过 HTTP 资源与真实 API 核验，而非仅看启动日志或页面可打开。

### 9.2 用户操作与视觉检查

追加响应式交付。对比 320、390、768、1024、1440 px 的页面，并在 390×520 上验证触摸、焦点、滚动和弹窗按钮。窄屏的表树可以折叠，Snapshot 图表随内容区换列，Tag 用视口适配的展示保留全字段及原操作。后端复测路径见 [响应式交付说明](tasks/dashboard-responsive-handoff.md)。

按第 4 节功能对照表逐项在浏览器中执行真实点击、输入、键盘、文件选择和滚动。接口调用、DOM 修改或注入状态不能代替用户操作；API/存储检查只用于确认操作结果。

- 覆盖管理员与受限角色、可写/只读/能力不支持；Paimon、Iceberg、Hive/Mixed 等当前有功能分支的格式须使用对应数据，不能用一种格式推断全部等价。
- 至少覆盖非空列表、空库、无结果、长名称/大 ID、多页 Tag、不同保留期模式、带 snapshot/branch/tag 引用、文件/统计/健康信息与操作记录。Catalog、数据库、表创建/Hive 升级及资源组/Optimizer 写操作在独立 fixture 中实际确认结果后清理。
- 表单逐项验证校验、reset、动态字段、上传成功/失败、取消；关键写入在慢响应下验证 pending 禁用、双击/双 Enter 仅一次请求、确认过期和失败后的可恢复性。
- 至少 `1440×900` 与 `1280×720` 两视口、中英文；关键页面及空态/loading/校验错误/长文本状态截图对照。检查列宽、对齐、截断、横纵滚动、按钮点击区域、遮罩、浮层定位/层级、Dialog footer、图标、图表 resize 和 Swagger 样式。
- 以 B0 的同状态截图和原有文案为参照，保持标题层级、导航选中态、卡片留白、表头和展开区对比、标签页顺序、表单提示及空态文字。基础控件可采用 Element Plus 风格，允许改善旧版窄视口下的截断。发现重叠、遮挡、溢出或提示文字被截断时修复并重新截图。
- 检查 Tab 顺序、焦点恢复、Escape、Enter、菜单/Select 键盘操作、中文输入法、屏幕可读名称；剪贴板覆盖 localhost 与普通远程 HTTP 等价的非安全上下文、成功/拒绝/回退路径及完整文本。
- 记录 Console、组件/响应式警告、unhandled rejection、Network、资源 404、意外重复请求、mock 拦截与浮层卸载残留。没有新增未解决问题，错误提示与实际状态一致。

基线已有问题须单独保存证据，不在本轮偷偷修改业务语义。例如前阶段观察到的时间显示差异、UNKNOWN 文本、非法 XML 路径异常和重复操作内存增长，只是待复核线索，须在本次固定基线上重新确认。新增问题必须修复；原有问题若阻碍迁移验收，提出最小修复及其范围影响再更新 Spec。

## 10. 成功标准与证据

| 编号 | 必须同时满足的条件 |
|---|---|
| AC-01 版本/依赖 | 精确采用 Element Plus `2.14.8` 和图标 `2.3.2`。四个旧包及其无用途配置从依赖与分发图中清理。验证 B0 工具链、peer 和 engine 兼容 |
| AC-02 功能完整 | 第 4 节功能对照逐项通过；路由、API、权限、能力、数据语义、键盘和请求次数无回归，无缺项被默认豁免 |
| AC-03 组件兼容 | 第 5 节所有实际用法、类型、反馈、表格/表单/上传/图标/SVG/剪贴板完成适配；不以双 UI 并存或全量导入交付 |
| AC-04 自动验证 | frozen 严格安装、两项类型检查、既有及新增必要测试、Vite 构建、Maven 分发验证通过；lint 无新增未解决问题，未弱化检查 |
| AC-05 真实运行 | 当前产物在真实本地 Amoro 正确加载，资源/进程身份、真实 API 和刷新后的写入结果可核查 |
| AC-06 视觉/操作 | 两视口、中英文及适用状态截图/点击回放齐全；基础控件风格变化符合约定，功能布局无错乱 |
| AC-07 解析收益 | P1 解析/编译 CPU 中位数至少降低 10%，超过噪声，有原始 trace、独占时间口径和重复采样证据 |
| AC-08 渲染收益 | P2 前端渲染区间中位数至少降低 10%，超过噪声；端到端时延及其他关键操作无可复现退化 |
| AC-09 运行稳定 | 无新增未解决的异常、重复请求、浮层/监听残留、卡顿或泄漏证据；历史风险与新增问题分列并说明边界 |
| AC-10 分发与报告 | 许可证与实际产物一致，审计差异已解释；报告包含完整版本/命令结果/覆盖矩阵/截图/trace/遗留风险，运行数据与敏感材料不入提交 |

任一必要功能、环境、性能指标或证据缺失时，状态为“待验收”，不能标记“迁移完成”。通过构建不能替代浏览器验收；通过功能回归也不能替代已确认的性能提升目标。

## 11. 边界与风险处理

**始终执行：**维护当前功能对照；依据固定版本官方文档及发布源码核查组件 API；只做必要的前端兼容适配；保留实际数据与测试；记录公平的基线和新增问题；验证依赖、许可与真实 AMS 产物；保证每次业务操作结果可追溯。

**先更新 Spec 并确认范围：**改变任何业务功能/交互/布局、权限、接口/数据含义、后端代码、Node/pnpm 基准、浏览器支持范围或验收目标；新增无关依赖/重做图表或虚拟列表；将本轮扩展成已有业务缺陷修复。已约定的 UI/图标/剪贴板/SVG 替换和必要前端适配不重复申请授权。

**禁止：**削减功能或数据换取速度；伪造成功/隐藏失败；修改生产数据；混入其他 worktree 变更；关闭类型/规则、删除有效测试或换用 mock 充当验收；手改锁文件；提交凭据/DB/日志/构建缓存；未经后续明确要求推送、创建新 PR、合并或部署。

| 风险 | 防止错误结论/交付的约束 |
|---|---|
| 新 UI 库未必比旧库快，按需导入可能被公共 chunk 抵消 | P1/P2 为独立硬指标；审查真实模块图/trace，未达目标不能结项 |
| 标准/虚拟表格、树和表单 API 不等价 | 默认标准组件；逐用法功能对照与边界测试，不能统一机械替换 |
| Element Plus 默认关闭/校验/清空行为不同 | 每处显式匹配现有语义，特别是异步写入、取消和空值 |
| CSS/浮层/语言切换只在特定状态错乱 | 多视口/语言/状态，含命令式实例、teleport 和卸载检查 |
| HTTP 剪贴板能力丢失、回退假成功 | 非安全上下文真实复制、结果核验与明确失败处理 |
| 旧静态资源、代码缓存或 mock 导致假通过 | 清理独立输出；HTTP/JAR/源码资源 hash 与进程核对，冷/热样本分离 |
| MR !66 或 dev 基线发生变化 | 不向现有 MR 混入迁移文档/代码；后续集成时重新固定基线并复验 |

## 12. 待审阅内容与来源

范围、视觉标准和“各至少提升 10%”目标已确认。本次更新覆盖版本选择、安装校验和对应回归任务。严格锁文件安装、真实 AMS Chrome 回归、主要页面视觉对照及两项性能门槛已验证。其他格式和权限组合按任务清单继续验证。

2026-11-20 核查的 `2.14.8` 官方 tag 和 npm `gitHead` 均指向 `da9cd14fe82cbffc10d7b1f460eb1e30619a2f27`。组件 API 以该 tag 和同版本发布包为准。

- 本地证据：本 Spec 指定提交下的 `package.json`、`pnpm-lock.yaml`、`pom.xml`、`vite.config.mts`、`src/main.ts`、`App.vue`、`router/index.ts`、全部 Vue/TS/样式及既有测试。前阶段归档 `/Users/SL/Documents/amoro-design/web-dependency-upgrades/` 仅用于背景与风险线索。
- [Element Plus 2.14.8 发布说明](https://github.com/element-plus/element-plus/releases/tag/2.14.8) · [对应版本源码](https://github.com/element-plus/element-plus/tree/2.14.8) · [精确发布包元数据](https://registry.npmjs.org/element-plus/2.14.8)
- [安装与浏览器兼容范围](https://element-plus.org/en-US/guide/installation.html)：官方最低版本表与最近两个版本建议不能代替项目当前浏览器支持契约；实施前记录现有支持矩阵，不得默默缩小。若发现与目标版本冲突，作为待决兼容问题提交审阅。
- [按需导入](https://element-plus.org/en-US/guide/quickstart.html)、[unplugin-vue-components 32.1.0 的 Element Plus resolver](https://github.com/unplugin/unplugin-vue-components/blob/v32.1.0/src/core/resolvers/element-plus.ts)。
- [固定版本 Form](https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/form.md) · [Table](https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/table.md) · [Dialog](https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/dialog.md) · [MessageBox](https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/message-box.md) · [Upload](https://github.com/element-plus/element-plus/blob/2.14.8/docs/en-US/component/upload.md)
- [国际化与 Day.js](https://element-plus.org/en-US/guide/i18n.html)、[图标用法](https://element-plus.org/en-US/component/icon.html)、[图标 2.3.2 发布包](https://registry.npmjs.org/@element-plus/icons-vue/2.3.2)。
- [Vite 静态资源](https://vite.dev/guide/assets.html)、[Element UI 迁移指南的实际适用范围](https://element-plus.org/en-US/guide/migration.html)。
- 旧包发布记录见 [Ant Design Vue](https://registry.npmjs.org/ant-design-vue/latest) · [Ant Design Vue 图标](https://registry.npmjs.org/@ant-design/icons-vue/latest) · [vue-clipboard3](https://registry.npmjs.org/vue-clipboard3/latest) · [SVG 插件](https://registry.npmjs.org/vite-plugin-svg-icons/latest)
