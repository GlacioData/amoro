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

# 任务清单：Element Plus 2.14.8 迁移

- 更新：2026-11-20，Asia/Singapore。**迁移代码已实施，18 项 Chrome 回归与两项主性能目标已通过**。
- 计划：[plan.md](plan.md)；需求：[Spec](../SPEC-element-plus-migration.md)。
- 任务是否完成以本清单为准。已取得的证据汇总于 [本地验证记录](../output/element-plus-verification/report.md)。
- 所有验收/验证项初始均未勾选；只有该任务及通用完成标准均通过才能勾选“任务完成”项。阻碍/证据记在任务旁，不能提前勾选。
- “W 目录”指 `/Users/SL/.codex/worktrees/element-plus-migration/amoro/amoro-web`；其他命令在仓库根运行。
- **V-web**：`pnpm run typecheck`、`pnpm exec vue-tsc --noEmit -p tsconfig.node.json`、`pnpm run build`（含现有测试）及 lint 相对基线的诊断比较；完整命令见 Plan §7。
- **V-dist / V-ams**：Plan §7 的当前分发构建与隔离真实 AMS 启动/身份核验。开发/mock 仅辅助，不代替正式验收。
- `output/element-plus-verification/` 下列出的工具与运行证据用于本地验证。数据、凭据、trace 和截图不入产品提交。
- 每项 1–5 个修改文件；只读参考/运行数据不计。实际超过 5 文件或一次聚焦会话，先在此展开小子任务，再执行，不压缩验收。
- 源码片段必须可构建且相关完整路径可操作；失败修复后复测。常规检查点记录证据供审阅，实施获批后不反复请求同一范围内授权。

## T01：固定实施基线 B0

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 读取并整合实施时的远端 `sl/dev`，在保留旧 UI 的状态下固定完整 Amoro 比较基线。

**验收标准：**
- [ ] 记录 `sl/dev` 和 B0 提交、前端树及锁文件哈希、实际组件版本和 Maven 分发版本。上游功能与运行配置差异均纳入对照。
- [ ] 生成模板、别名、类型、图标、样式、命令式调用和测试选择器清单，每项有迁移任务或保留依据。
- [ ] 复核 Node/pnpm/JDK 与浏览器支持矩阵；基线 install/typecheck/build/lint/audit 的真实结果已冻结。

**验证：**
- [ ] 执行 Plan §7 的环境、严格安装、类型、构建、lint/audit 命令并保留退出码。
- [ ] 用 git diff 核对 `sl/dev` 和 B0 的前端、后端与运行配置，确认 B0 使用旧 UI。复核共享基线的 CORS 配置与分发路径。

**依赖：** 无。

**预计修改文件：**
- `SPEC-element-plus-migration.md`
- `output/element-plus-verification/baseline.json`
- `output/element-plus-verification/inventory.json`

**范围：** M（3 个文件；验证产物不入提交）。

## T02：准备隔离 AMS 与 fixture

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 为 B0 与候选准备独立端口、数据快照、真实格式和权限数据；确认浏览器驱动可用。

**验收标准：**
- [ ] Paimon/Iceberg/Hive-Mixed 能力分支、管理员/受限/只读、可执行写操作均有真实 fixture 或明确缺口。
- [ ] 覆盖空库、长名称、大 ID、多页 Tag、snapshot/branch/files/health/operation，以及 ≥1,000 可访问节点；两端可复原同状态。
- [ ] 记录端口、DB/metastore/warehouse/fixture hash、工具版本及同源访问方式。隔离 AMS 配置非默认管理员密码，记录中仅保留配置项名称。

**验证：**
- [ ] 执行 V-dist/V-ams，从 AMS 同源页面登录并核对 API、格式、权限和 fixture 数量。若另用 Vite 跨源调试，验证本地代理或明确的 Origin 配置。
- [ ] 恢复一次 fixture 快照后复核内容；不足环境作为前置缺口处理，不能改用 mock。

**依赖：** T01。

**预计修改文件：**
- `output/element-plus-verification/fixtures.json`
- `output/element-plus-verification/tools/runtime-fixture.mjs`
- `output/element-plus-verification/parity.json`

**范围：** M（3 个文件；验证产物不入提交）。

## T03：验证性能采集与统计工具

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 建立 Plan §6/§7 的本地 trace 驱动和分析器，在迁移前证明目标指标可测。

**验收标准：**
- [ ] 采集器实现声明的 CLI、隔离缓存/会话、单调时间轴、P1/P2 区间和串行浏览器运行。
- [ ] 分析自测覆盖嵌套独占时间、单位/线程、缺失/零/无效样本、分位数、配对 bootstrap/固定 seed。
- [ ] 至少一条真实 trace 与 DevTools 对照；可识别相应完成绘制帧，不能用 LCP/固定等待/RAF 替代。

**验证：**
- [ ] node --test output/element-plus-verification/tools/trace-analysis.test.mjs。
- [ ] 在 B0 采集预检 trace，人工核对事件、请求集合和结束帧；无对应事件即前置失败。

**依赖：** T02。

**预计修改文件：**
- `output/element-plus-verification/tools/trace-capture.mjs`
- `output/element-plus-verification/tools/trace-analyze.mjs`
- `output/element-plus-verification/tools/trace-analysis.test.mjs`
- `output/element-plus-verification/workload.json`

**范围：** M（4 个文件；验证产物不入提交）。

### 检查点 CP01：T01–T03

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T04：冻结身份导航与 Catalog 基线

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 通过真实 UI 记录身份、路由、Catalog 和数据库写操作的契约与回放。

**验收标准：**
- [ ] 登录/失效/退出、角色门控、Logo/菜单 push-replace、历史/刷新/404/旧 Hive URL 都有基线记录。
- [ ] Catalog 创建/编辑/删除、动态配置、点号键、属性、校验、上传/下载与数据库创建已记录请求和刷新结果。
- [ ] 中英文及成功/取消/loading/失败状态可重放，历史问题与待验收缺口分列。

**验证：**
- [ ] 在 B0 运行 cases/identity-catalog.mjs 对应 UI 用例并保存 Console/Network。
- [ ] 核对 parity.json 的请求/结果和数据恢复，不能仅确认弹窗可打开。

**依赖：** T03。

**预计修改文件：**
- `output/element-plus-verification/cases/identity-catalog.mjs`
- `output/element-plus-verification/parity.json`
- `output/element-plus-verification/baseline/identity-catalog.json`

**范围：** M（3 个文件；验证产物不入提交）。

## T05：冻结表功能基线

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 冻结表入口、详情健康、各快照分支、Tag 和 Hive 路径的操作及数据预期。

**验收标准：**
- [ ] 树/旧菜单、创建/升级、Details/TableModel/健康、Files/Snapshots/Selector 各格式分支均有明确用例。
- [ ] Tag 创建/复制/删除/保留期、观察 token/二次确认/过期/不确定结果、只读能力和双 Enter 请求计数已覆盖。
- [ ] 大 ID/空值/时间/单位、错误/429/过期响应与刷新状态已记录；P2 固定表和必要请求集合已确定。

**验证：**
- [ ] 在 B0 执行 cases/tables.mjs 的真实点击和写操作，保留 API 与截图证据。
- [ ] 复核相同 fixture 可再次执行；不可用功能不得以空页面或单一格式替代。

**依赖：** T04。

**预计修改文件：**
- `output/element-plus-verification/cases/tables.mjs`
- `output/element-plus-verification/parity.json`
- `output/element-plus-verification/baseline/tables.json`

**范围：** M（3 个文件；验证产物不入提交）。

## T06：冻结资源与公共页面基线

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 记录当前 Resource 三标签页、管理操作、Overview/Settings/Swagger 等剩余路径。

**验收标准：**
- [ ] 优化/进程/资源组/Optimizer 创建、编辑、释放/删除、扩缩容、筛选/分页和确认有真实结果。
- [ ] Overview 全部卡片/图表与时间范围、Settings 折叠/列表、Swagger schema/CSS、操作文本复制都有基线。
- [ ] 公共 loading/tooltip/locale/焦点和卸载状态被记录；路由未启用的旧文件不恢复路由但纳入编译库存。

**验证：**
- [ ] 在 B0 执行 cases/management.mjs，检查真实请求、写入刷新与图表 resize。
- [ ] 核对 Spec §4 的功能域全部有 parity ID；浏览器支持范围/尚缺环境单独列明。

**依赖：** T05。

**预计修改文件：**
- `output/element-plus-verification/cases/management.mjs`
- `output/element-plus-verification/parity.json`
- `output/element-plus-verification/baseline/management.json`

**范围：** M（3 个文件；验证产物不入提交）。

### 检查点 CP02：T04–T06

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T07：冻结性能 A/A 与测量合同

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 使用已验证的 B0 工具测量 P1/P2/P3/P5，先确定噪声和无效样本规则。

**验收标准：**
- [ ] B0 同条件 A/A 各至少 10 个有效样本，原始 trace、p50/p95/IQR、噪声和排除理由齐全。
- [ ] workload 的表/模式/页大小/请求集合、测量区间、驱动 hash、缓存/profile/语言/视口与统计算法均冻结。
- [ ] 测量工具/fixture 缺口闭环；无有效 parse/compile 或绘制指标时不允许进入 T08。

**验证：**
- [ ] 执行 Plan §7 baseline 采样和分析接口，复核 P1/P2 分别可测。
- [ ] 人工检查一条慢样本及一条普通样本，确认没有用工具等待或后端时间冒充前端收益。

**依赖：** T06。

**预计修改文件：**
- `output/element-plus-verification/workload.json`
- `output/element-plus-verification/baseline/aa-summary.json`
- `output/element-plus-verification/baseline/measurement-contract.md`

**范围：** M（3 个文件；验证产物不入提交）。

## T08：引入按需组件基础

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 精确加入 Element Plus `2.14.8` 和图标 `2.3.2`，配置按需 CSS、指令和主题入口。旧页面在迁移完成前继续使用现有控件。

**验收标准：**
- [ ] 直接依赖精确锁定 `element-plus@2.14.8` 和 `@element-plus/icons-vue@2.3.2`。核对 Vue 和图标的实际解析及重复实例，记录 `@vueuse/core@15.0.0` 和 `@ctrl/tinycolor` 的实际版本与路径。原有基础依赖沿用 B0 锁定版本。
- [ ] 旧、新 resolver 临时共存。通过实际构建确认模板组件、指令和命令式 Message/MessageBox/Loading 的按需 CSS，以及中英文 locale 入口均可解析。
- [ ] 主题/reset 顺序不破坏旧页面、原有路径/base/outDir/preload 与开发/mock 行为。

**验证：**
- [ ] 按 Plan §7 生成锁文件，执行严格 frozen 安装和 V-web。核对 peer、engine、依赖路径及生成的 components.d.ts。
- [ ] 本地 AMS 回放登录/表入口/Swagger，查看实际模块与 CSS 加载。

**依赖：** T07。

**预计修改文件：**
- `amoro-web/package.json`
- `amoro-web/pnpm-lock.yaml`
- `amoro-web/vite.config.mts`
- `amoro-web/src/styles/element-plus.less`
- `amoro-web/src/styles/index.less`

**范围：** M（5 个文件；验证产物不入提交）。

## T09：适配 locale 与公共反馈

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 在 App 和请求/登录路径使用新 locale 和窄反馈接口，保留原鉴权逻辑。

**验收标准：**
- [ ] ElConfigProvider、中英文与 Day.js 联动；过渡期嵌套保留旧 ConfigProvider 同步 locale，JS 反馈 CSS/文案/取消 Promise 正确。
- [ ] request.ts 仅替换反馈入口，保留登录失效去重、reject/跳转、headers/cancel/rawData 语义。
- [ ] 反馈契约测试覆盖成功/取消/失败和重复登录提示；不建立完整旧 Modal 兼容库。

**验证：**
- [ ] V-web；运行 node --experimental-vm-modules scripts/ui-feedback.test.mjs（W 目录，拟新增）。
- [ ] 真实 AMS 登录/失效/拒绝/重登录及语言切换，无新增 unhandled rejection。

**依赖：** T08。

**预计修改文件：**
- `amoro-web/src/App.vue`
- `amoro-web/src/utils/ui-feedback.ts`
- `amoro-web/src/utils/request.ts`
- `amoro-web/src/views/login/index.vue`
- `amoro-web/scripts/ui-feedback.test.mjs`

**范围：** M（5 个文件；验证产物不入提交）。

### 检查点 CP03：T07–T09

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T10：迁移操作详情与复制

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 以 Operations 完整用户路径验证 Table/Dialog 和平台剪贴板封装。

**验收标准：**
- [ ] Operations 列/分页/详情 Dialog、关闭和成功后流程不变，复制完整多行/Unicode/长文本。
- [ ] Clipboard API 不支持/拒绝时受控回退，返回 false/抛异常不得显示成功；焦点/选区/临时节点恢复。
- [ ] 新增行为测试覆盖两条复制路径与失败；暂留旧声明供最终依赖图统一清理。

**验证：**
- [ ] V-web；node --test scripts/clipboard.test.mjs（W 目录，拟新增）。
- [ ] 真实浏览器核对剪贴板文本、普通 HTTP 场景和失败后的可操作性。检查详情 Dialog 自定义标题的可访问名称及关闭后的焦点恢复。

**依赖：** T09。

**预计修改文件：**
- `amoro-web/src/utils/clipboard.ts`
- `amoro-web/scripts/clipboard.test.mjs`
- `amoro-web/src/views/tables/components/Operations.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T11：迁移导航外壳

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Topbar、Sidebar 与 404 控件和图标，保留所有导航/权限行为。

**验收标准：**
- [ ] 菜单、折叠、active 状态、版本/文档/语言/退出、确认和图标操作含义一致。
- [ ] Logo/侧栏和浏览器历史行为、旧 URL 与权限默认路由不变，未修改路由/权限模块。
- [ ] 两视口和中英文无遮挡。用侧栏原有间距检查菜单展开、收起和选中切换时有无跳动，并核对 tooltip、键盘和焦点。

**验证：**
- [ ] V-web；node --experimental-vm-modules scripts/dashboard-navigation.test.mjs（W）。
- [ ] V-ams 回放 identity 用例并核对 query、history、权限和退出请求。

**依赖：** T10。

**预计修改文件：**
- `amoro-web/src/components/Topbar.vue`
- `amoro-web/src/components/Sidebar.vue`
- `amoro-web/src/views/404.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T12：迁移公共加载态

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 将 ULoading 与 ECharts 容器的 Spin 迁移，保持生命周期和遮罩范围。

**验收标准：**
- [ ] ULoading 的公开 props/注册方式、局部/全屏遮罩、文字与层级一致。
- [ ] Chart loading、空态、resize、现有数据主题与 ECharts 注册不变。
- [ ] 卸载后无残留遮罩/监听，新旧页面加载切换仍正常。

**验证：**
- [ ] V-web；检查原组件 API 与新的 Loading/实例清理分支。
- [ ] V-ams 回放 Overview 与表详情加载/错误/切页及连续 resize。

**依赖：** T11。

**预计修改文件：**
- `amoro-web/src/components/loading/loading.vue`
- `amoro-web/src/components/loading/index.ts`
- `amoro-web/src/components/echarts/Chart.vue`

**范围：** M（3 个文件；验证产物不入提交）。

### 检查点 CP04：T10–T12

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T13：迁移 Catalog 列表与属性

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Catalog 列表反馈和可编辑属性表格，保留已有父子组件契约。

**验收标准：**
- [ ] 列表权限、空态、选中、删除/取消/状态检查与原路由相同。
- [ ] 属性添加/删除/编辑、重复/空值判断与 getProperties/getPropertiesWithoputValidation 等原接口不变。
- [ ] 剩余旧 Detail 在过渡期仍可完整保存，属性结果与基线一致。

**验证：**
- [ ] V-web；回放 identity-catalog 的列表、属性校验、保存和取消用例。
- [ ] 核对父组件收到的结果、真实请求字段与刷新显示，不能将点号键拆分。

**依赖：** T12。

**预计修改文件：**
- `amoro-web/src/views/catalogs/index.vue`
- `amoro-web/src/views/catalogs/Properties.vue`

**范围：** S（2 个文件；验证产物不入提交）。

## T14：迁移 Catalog 动态表单

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 仅迁移 Detail 的表单、选择、校验和保存/取消布局，上传控件留到下一任务。

**验收标准：**
- [ ] 各种类型/格式/存储/认证下字段显隐、必填/disabled、属性与优化组选择不变。
- [ ] 数组 prop 保留字面点号键，validate/reset/清空值与保存 payload、错误/取消结果一致。
- [ ] 临时仍使用旧上传时流程仍工作，未重写服务请求或业务规则。

**验证：**
- [ ] 运行 V-web，按基线表单矩阵触发校验、取消和成功保存。覆盖下拉打开时其父控件变为禁用，核对候选项无法再选中、焦点状态释放且请求次数正确。
- [ ] 逐字段比对 API body 与 reset 后状态，含 SIMPLE/KERBEROS、可选文件等路径。

**依赖：** T13。

**预计修改文件：**
- `amoro-web/src/views/catalogs/Detail.vue`

**范围：** S（1 个文件；验证产物不入提交）。

## T15：迁移 Catalog 上传

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 完成 Detail 的 Upload/类型/事件适配，验证真实文件状态和业务返回。

**验收标准：**
- [ ] 上传 URL/multipart/请求头/认证、可选/必填、文件回显/下载/删除/重传与取消一致。
- [ ] HTTP 成功与业务 code 分别判断；非法 XML、上传中/失败结果不误绑定有效 file ID。
- [ ] 保存正确配置、取消/失败恢复与原有规则一致，新增异常闭环，历史问题单列。

**验证：**
- [ ] V-web；真实 AMS 上传合法/非法 XML 及认证文件，保存并刷新核对。
- [ ] 检查 Network 请求、业务响应和 fileId，重复上传/失败/慢响应下没有假成功。

**依赖：** T14。

**预计修改文件：**
- `amoro-web/src/views/catalogs/Detail.vue`

**范围：** S（1 个文件；验证产物不入提交）。

### 检查点 CP05：T13–T15

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T16：迁移分层表树

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 TableExplorer 的 Tree、搜索与按钮，保留原数据加载与状态算法。

**验收标准：**
- [ ] catalog/database/table 懒加载、单词/层级搜索、防抖、刷新及无结果与原行为一致。
- [ ] 展开/选中/sessionStorage、键盘/滚动位置、切换过期响应及 loading 不变。
- [ ] ≥1,000 节点场景仍可访问全部数据，未扩大请求/删节点或移除能力。

**验证：**
- [ ] V-web；聚焦现有 table-explorer-state.test.mts；回放树用例。
- [ ] 真实 AMS 快速切换/搜索/刷新/展开及大列表滚动，核对请求次数和可见数据。

**依赖：** T15。

**预计修改文件：**
- `amoro-web/src/views/tables/components/TableExplorer.vue`

**范围：** S（1 个文件；验证产物不入提交）。

## T17：迁移旧菜单与数据库创建

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 TablesMenu、RecycleScroller 的空态及 CreateDB 表单/弹窗。

**验收标准：**
- [ ] 旧菜单搜索/catalog/database 切换、虚拟滚动与 active 事件契约不变。
- [ ] 保留 RecycleScroller，不替换维护中的虚拟列表实现或删除本地格式图标。
- [ ] CreateDB 必填/取消/提交/刷新、权限及焦点正确，分页/搜索没有双请求。

**验证：**
- [ ] V-web；在真实 AMS 回放旧入口和数据库创建成功/失败/取消。
- [ ] 连续滚动检查行高/空白/重叠，检查 locale、图标和 request 结果。

**依赖：** T16。

**预计修改文件：**
- `amoro-web/src/components/tables-sub-menu/TablesMenu.vue`
- `amoro-web/src/components/tables-sub-menu/CreateDB.vue`
- `amoro-web/src/components/VirtualRecycleScroller.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T18：迁移表页面容器

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Tables tabs 和 Create 表单，保持路由与子组件挂载行为。

**验收标准：**
- [ ] 各 tab、secondary nav、query、创建/升级入口和权限/能力显示不变。
- [ ] 原销毁非活动内容语义按活动 key 显式实现，不把 lazy 当作 destroy；切页请求/重置等价。
- [ ] Create 字段、清空值、校验和父事件相同，不新增路由或改变创建流程。

**验证：**
- [ ] V-web；现有导航/快照页面状态测试保持；回放 tables 容器与创建用例。
- [ ] 真实 AMS 多次切 tab/表及历史前进后退，核对数据与挂载/请求次数。

**依赖：** T17。

**预计修改文件：**
- `amoro-web/src/views/tables/index.vue`
- `amoro-web/src/views/tables/create.vue`

**范围：** S（2 个文件；验证产物不入提交）。

### 检查点 CP06：T16–T18

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T19：迁移表详情展示

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Details 表格/类型/tooltip 与 TableModel，保留数据模型及统计语义。

**验收标准：**
- [ ] 列、字段/分区/属性、空态、文件统计、单位/精度/时间和大 ID 展示等价。
- [ ] 移除视图中的 Ant ColumnProps，用目标类型；不改业务 IColumns、normalizeTableModel 或服务返回。
- [ ] 初始化失败/表不存在/请求身份与跳转行为不变，健康信息入口保持。

**验证：**
- [ ] V-web；现有 table-model/file-statistics-view 与 snapshot-page-state 测试。
- [ ] 真实 AMS 按格式比对详情数值、列/tooltip、错误路径和刷新。在实际动态列切换处核对表头及 header slot 随列更新。

**依赖：** T18。

**预计修改文件：**
- `amoro-web/src/views/tables/components/Details.vue`
- `amoro-web/src/views/tables/components/TableModel.vue`

**范围：** S（2 个文件；验证产物不入提交）。

## T20：迁移健康信息路径

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移健康详情 Dialog、Progress/Tooltip 和 Paimon 结构/对比展示。

**验收标准：**
- [ ] 评分、阈值、unknown/空态、结构/对比、数值与单位沿用原业务结果。
- [ ] 弹窗、折叠、表格/图标/tooltip 和关闭交互不变，不因 Progress 范围改变原数据。
- [ ] 大表/长文本/两视口显示完整，不新增重复请求或隐藏信息。

**验证：**
- [ ] V-web；真实 AMS 回放 Details→健康信息/结构/对比及弹窗取消。
- [ ] 同数据截图和数值逐项比对，检查卸载/resize/locale。

**依赖：** T19。

**预计修改文件：**
- `amoro-web/src/views/tables/components/HealthScoreDetails.vue`
- `amoro-web/src/views/tables/components/PaimonHealthStructure.vue`
- `amoro-web/src/views/tables/components/PaimonHealthComparison.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T21：迁移快照引用选择

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Snapshot wrapper、Selector 多标签浮层和 TagSnapshotSelect。

**验收标准：**
- [ ] branch/tag/consumer 的搜索、分页、选择/关闭、operation 与事件 payload 保持原值类型/含义。
- [ ] ElPopover 内搜索/Select 不误关闭，pending 时选择/分页禁用与父事件一致，teleport/滚动定位正确。
- [ ] bounded/legacy 分流、能力/只读/失败重试、过期响应和 tag 大 ID 不变。

**验证：**
- [ ] V-web；现有 tag-snapshot-state 与 snapshot-page-state 测试。
- [ ] 真实 AMS 回放引用切换、Tag page2、consumer、快速切表/失败/慢响应与键盘。

**依赖：** T20。

**预计修改文件：**
- `amoro-web/src/views/tables/components/Snapshots.vue`
- `amoro-web/src/views/tables/components/Selector.vue`
- `amoro-web/src/views/tables/components/TagSnapshotSelect.vue`

**范围：** M（3 个文件；验证产物不入提交）。

### 检查点 CP07：T19–T21

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T22：迁移有界快照路径

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 BoundedSnapshots 的表格、统计、面包屑、展开和图标。

**验收标准：**
- [ ] 快照/文件详情、统计、行 key/大 ID、页大小与原列/展开/选择完整保留。
- [ ] 固定 P2 场景与请求集合不变，绘制区间可测，不能为提速删列/统计或提前结束等待。
- [ ] loading/错误/429/失效、引用定位与刷新行为等价。

**验证：**
- [ ] V-web；现有 snapshot-chart-data/snapshot-page-state 测试。
- [ ] 真实 AMS 重放 P2 功能路径与展开/返回，保存候选预检 trace 供归因，不在双库阶段判最终收益。

**依赖：** T21。

**预计修改文件：**
- `amoro-web/src/views/tables/components/BoundedSnapshots.vue`

**范围：** S（1 个文件；验证产物不入提交）。

## T23：迁移旧快照与文件详情

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 LegacySnapshots 和 Files，保持旧能力分支及文件分页搜索。

**验收标准：**
- [ ] 旧 snapshot 分支、base/change、图表/行展开/面包屑与数据含义不变。
- [ ] Files 搜索/清空/分页/详情/tooltip、空值与总量一致，一次操作仅发原必要请求。
- [ ] 格式不支持/空态/失效/快速切换处理等价，没有因新版快照可用而删除旧路径。

**验证：**
- [ ] V-web；现有快照/文件统计测试；在真实支持旧能力的 fixture 回放。
- [ ] 核对 API 参数、完整行数/面包屑和图表，不能拿 bounded 成功代替 legacy 覆盖。

**依赖：** T22。

**预计修改文件：**
- `amoro-web/src/views/tables/components/LegacySnapshots.vue`
- `amoro-web/src/views/tables/components/Files.vue`

**范围：** S（2 个文件；验证产物不入提交）。

## T24：迁移 Tag 管理页面

**本地证据。** 真实 AMS 已回放创建、搜索、二次确认复制、输入名称确认删除。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Tags 表格、搜索/分页、创建/复制/删除弹窗和反馈。

**验收标准：**
- [ ] 默认 20 和 20/50/100 页大小、query、搜索/总数、snapshot 跳转与按钮门控相同。
- [ ] 创建/复制确认/删除名称确认、busy、双 Enter、token/确认过期及结果不确定语义不变。
- [ ] 错误消息分层、失败关闭/刷新与成功后持久状态一致，不放宽表名/版本/保留期校验。

**验证：**
- [ ] V-web；现有 tag-management-state/tag-snapshot-state 测试。
- [ ] 在真实 AMS 创建、复制、删除并刷新，测试重名、只读和慢响应。核对一次写请求及各 Dialog 的可访问名称和关闭焦点。

**依赖：** T23。

**预计修改文件：**
- `amoro-web/src/views/tables/components/Tags.vue`

**范围：** S（1 个文件；验证产物不入提交）。

### 检查点 CP08：T22–T24

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T25：迁移 Tag 保留期对话框

**本地证据。** 真实 AMS 已将临时 Tag 的保留期改为 48 小时并在列表确认。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 TagRetentionModal，保持先捕获确认数据再提交的规则。

**验收标准：**
- [ ] 模式/小时数/原始空值、native default、clock 与前后策略文本一致。
- [ ] 确认捕获 row/scope/expected/token 不被控件默认值重推断，返回编辑/取消/busy 关闭语义不变。
- [ ] 成功/失败/过期/只读、父事件与刷新结果保持，不能把 metadata 更新说成历史清理。

**验证：**
- [ ] V-web；现有 Tag 状态测试；实际更新并刷新检查保留期。
- [ ] 慢响应双 Enter 与关闭/返回编辑路径，核对请求体和权限。检查 Select 动态禁用和焦点、InputNumber 数值及其 ARIA 属性、Dialog 自定义标题关联。

**依赖：** T24。

**预计修改文件：**
- `amoro-web/src/views/tables/components/TagRetentionModal.vue`

**范围：** S（1 个文件；验证产物不入提交）。

## T26：迁移表优化管理

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移表级 Optimizing 的列表、详情/确认弹窗、选择和加载态。

**验收标准：**
- [ ] 列表/时间/状态/列/分页/面包屑与优化操作入口、权限保持。
- [ ] 确认/取消/失败/成功刷新和请求次数不变，不重写优化状态算法。
- [ ] tooltip/长文本/浮层/图标、loading 和卸载无回归。

**验证：**
- [ ] V-web；真实 AMS 支持的优化 fixture 实际操作并核对结果。
- [ ] 检查 busy、防重复请求、表切换失效和两视口布局。

**依赖：** T25。

**预计修改文件：**
- `amoro-web/src/views/tables/components/Optimizing.vue`

**范围：** S（1 个文件；验证产物不入提交）。

## T27：迁移 Process 列表

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Process 的筛选、表格和 tooltip，保持原过滤/分页行为。

**验收标准：**
- [ ] 筛选、列、总数、页码、空态和状态/时间/大小展示一致。
- [ ] loading/失败与快速切换不显示过期记录，业务请求未改变。
- [ ] 控件清空值与原参数一致，无重复请求和滚动布局回归。

**验证：**
- [ ] V-web；真实 AMS 回放 process 的筛选/清空/切表/分页。
- [ ] 对比请求参数和显示总量，检查 tooltip 与 locale。

**依赖：** T26。

**预计修改文件：**
- `amoro-web/src/views/tables/components/Process.vue`

**范围：** S（1 个文件；验证产物不入提交）。

### 检查点 CP09：T25–T27

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T28：迁移 Hive 详情读路径

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移当前嵌入及保留旧文件中的 Hive tabs、基础/字段/分区表格。

**验收标准：**
- [ ] 所有实际可达 Hive Details/Field/Partition 信息、勾选和 tabs/父事件相同。
- [ ] 未启用的旧入口只保持编译/组件兼容，不恢复路由或删除能力。
- [ ] 原权限、数据类型、生命周期、空态与布局保持。

**验证：**
- [ ] V-web；真实 Hive/Mixed fixture 回放可达读路径。
- [ ] 核对字段/分区和 tabs 切换/勾选结果，记录旧文件的可达性依据。

**依赖：** T27。

**预计修改文件：**
- `amoro-web/src/views/tables/components/HiveDetails.vue`
- `amoro-web/src/views/hive-details/index.vue`
- `amoro-web/src/views/hive-details/components/Details.vue`
- `amoro-web/src/views/hive-details/components/Field.vue`
- `amoro-web/src/views/hive-details/components/Partition.vue`

**范围：** M（5 个文件；验证产物不入提交）。

## T29：迁移 Hive 配置与升级

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Hive 属性表单、错误弹窗及升级表单的可用操作。

**验收标准：**
- [ ] 属性自动补全/添加/删除、必填/清空/保存与错误详情显示不变。
- [ ] 升级入口、权限、确认/取消及真实请求结果相同，不修改服务/表格式语义。
- [ ] 普通/失败/慢响应可恢复，焦点及弹窗关闭没有新增异常。

**验证：**
- [ ] V-web；真实本地 Hive fixture 实际保存/升级并检查刷新结果。
- [ ] 回放无权限/校验失败/后端失败和取消；缺环境不得默认通过。

**依赖：** T28。

**预计修改文件：**
- `amoro-web/src/views/hive-details/components/Properties.vue`
- `amoro-web/src/views/hive-details/components/ErrorMsg.vue`
- `amoro-web/src/views/hive-details/upgrade.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T30：迁移优化列表路径

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移当前 Resource 导入的 optimize/List 及旧容器/ScaleOut 编译路径。

**验收标准：**
- [ ] 当前列表、筛选/重置、分页、释放确认、external 禁用和数值格式一致。
- [ ] Typography copyable 的实际文本/tooltip/成功行为用受控复制实现保留，不能换成不可复制纯文本。
- [ ] 旧容器/ScaleOut 编译兼容且不恢复旧路由；无重复写入/消息。

**验证：**
- [ ] V-web；在当前 /optimizing 的 Tables tab 回放筛选/复制/释放。
- [ ] 真实 API 与刷新核对；旧组件单独验证实际保留的表单契约。

**依赖：** T29、T10。

**预计修改文件：**
- `amoro-web/src/views/optimize/components/List.vue`
- `amoro-web/src/views/optimize/components/ScaleOut.vue`
- `amoro-web/src/views/optimize/index.vue`

**范围：** M（3 个文件；验证产物不入提交）。

### 检查点 CP10：T28–T30

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T31：迁移资源操作表单

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移资源组、创建 Optimizer、扩缩容的表单 Dialog。

**验收标准：**
- [ ] 公开 props/emits、默认值、必填/选项/数值和新建/编辑规则相同。
- [ ] 确认/取消/忙碌/失败/成功后父刷新保持，权限与容器条件不放宽。
- [ ] 慢请求双提交、焦点、Escape/mask 和卸载无回归。

**验证：**
- [ ] V-web；真实测试资源组/Optimizer 执行新建/编辑/扩缩容和刷新。
- [ ] 比对 payload、一次写请求及失败/取消状态，结束恢复 fixture。

**依赖：** T30。

**预计修改文件：**
- `amoro-web/src/views/resource/components/GroupModal.vue`
- `amoro-web/src/views/resource/components/CreateOptimizerModal.vue`
- `amoro-web/src/views/resource/components/ScaleOut.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T32：迁移资源页容器与列表

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Resource 三标签页和组/Optimizer 列表及反馈。

**验收标准：**
- [ ] query/active tab、原销毁语义、默认入口、权限和创建/编辑触发等价。
- [ ] 列表列/分页/筛选、删除/释放/不能删除告警、挂载/卸载和刷新正确。
- [ ] 新 Dialog 与父状态相连，取消/错误不额外请求、不遗留浮层。

**验证：**
- [ ] V-web；运行 management 用例的三个 tab 与全部可用写操作。
- [ ] 核对真实请求、历史状态、分页页大小、文案/焦点和刷新。

**依赖：** T31。

**预计修改文件：**
- `amoro-web/src/views/resource/index.vue`
- `amoro-web/src/views/resource/components/List.vue`

**范围：** S（2 个文件；验证产物不入提交）。

## T33：迁移 Overview 基础展示

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 Overview 网格和统计/饼图基础卡片。

**验收标准：**
- [ ] 所有卡片/信息层级/位置、统计格式/单位/unknown 与原布局一致。
- [ ] ECharts 数据/主题/legend/tooltip/resize 不变，不替换维护中的图表库。
- [ ] 两视口/中英文/空态/loading 无挤压或组件高度错误。

**验证：**
- [ ] V-web；真实 AMS 回放 Overview 基础卡并截图比对。
- [ ] 核对统计值和图表数据，调整窗口检查 resize/卸载监听。

**依赖：** T32。

**预计修改文件：**
- `amoro-web/src/views/overview/index.vue`
- `amoro-web/src/views/overview/components/MultipleDataCard.vue`
- `amoro-web/src/views/overview/components/SingleDataCard.vue`
- `amoro-web/src/views/overview/components/PieChartCard.vue`

**范围：** M（4 个文件；验证产物不入提交）。

### 检查点 CP11：T31–T33

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T34：迁移 Overview 范围与排名

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移 DataSize/ResourceUsage 的 Select、Card 和 Top10 表格。

**验收标准：**
- [ ] 时间范围、请求时间单位、图表数据和 Top10 排序/列/链接保持。
- [ ] 选择/清空的值类型、一次 change 请求与 loading 不变，没有删卡片/点位换速度。
- [ ] tooltip/legend/resize 和分页/空态正常，布局和信息完整。

**验证：**
- [ ] V-web；真实 AMS 切换每个可用范围并核对必要请求与图表数据。
- [ ] 执行 Overview resize/hover 和 Top10 跳转，保存两视口结果。

**依赖：** T33。

**预计修改文件：**
- `amoro-web/src/views/overview/components/DataSizeCard.vue`
- `amoro-web/src/views/overview/components/ResourceUsageCard.vue`
- `amoro-web/src/views/overview/components/Top10TablesCard.vue`

**范围：** M（3 个文件；验证产物不入提交）。

## T35：迁移 Settings

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 迁移设置 tabs、Collapse 与表格，保持当前可用设置内容。

**验收标准：**
- [ ] 配置行、容器分组/折叠、active tab、文案和只读/可用操作一致。
- [ ] 切换/展开生命周期与请求次数保持，没有漏行/重复渲染。
- [ ] 长值/英文与两视口无列溢出或浮层遮挡。

**验证：**
- [ ] V-web；真实 AMS 对照配置数量与容器折叠/展开。
- [ ] 切页往返/刷新检查状态和 Console/Network。

**依赖：** T34。

**预计修改文件：**
- `amoro-web/src/views/settings/index.vue`

**范围：** S（1 个文件；验证产物不入提交）。

## T36：转换原生 SVG：品牌与基础图标

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 把 amoro、arctic、branch、catalogs 四个本地素材转换为同源 sprite 中的 SVG symbol，原始素材保留。

**验收标准：**
- [ ] viewBox/path/颜色/尺寸/内部引用与原始素材一致，保留品牌/格式可辨识性与来源。
- [ ] 有局部 ID/clipPath 的素材采用每个 symbol 独立前缀并同步引用，无 v-html/DOMParser/新插件。
- [ ] 对照重复引用时的显示、stroke/currentColor 和页面交互。

**验证：**
- [ ] V-web；使用本地预览逐一对照原 SVG 的正常/hover/disabled 视觉。
- [ ] 检查重复实例 ID 和 clip 引用、编译输出与包体；新增预览工具只放 output，不加产品入口。

**依赖：** T35。

**预计修改文件：**
- `amoro-web/src/assets/icons/sprite.svg`
- `output/element-plus-verification/tools/svg-preview.mjs`

**范围：** M（同一个 sprite 和本地预览工具；验证产物不入提交）。

### 检查点 CP12：T34–T36

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T37：转换原生 SVG：数据库与格式图标

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 把 database、format、hive、iceberg 四个本地素材转换为同源 sprite 中的 SVG symbol，原始素材保留。

**验收标准：**
- [ ] viewBox/path/颜色/尺寸/内部引用与原始素材一致，保留品牌/格式可辨识性与来源。
- [ ] 有局部 ID/clipPath 的素材采用每个 symbol 独立前缀并同步引用，无 v-html/DOMParser/新插件。
- [ ] 对照重复引用时的显示、stroke/currentColor 和页面交互。

**验证：**
- [ ] V-web；使用本地预览逐一对照原 SVG 的正常/hover/disabled 视觉。
- [ ] 检查重复实例 ID 和 clip 引用、编译输出与包体；新增预览工具只放 output，不加产品入口。

**依赖：** T36。

**预计修改文件：**
- `amoro-web/src/assets/icons/sprite.svg`

**范围：** S（同一个 sprite；验证产物不入提交）。

## T38：转换原生 SVG：管理菜单图标

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 把 optimizers、optimizing、overview、settings 四个本地素材转换为同源 sprite 中的 SVG symbol，原始素材保留。

**验收标准：**
- [ ] viewBox/path/颜色/尺寸/内部引用与原始素材一致，保留品牌/格式可辨识性与来源。
- [ ] 有局部 ID/clipPath 的素材采用每个 symbol 独立前缀并同步引用，无 v-html/DOMParser/新插件。
- [ ] 对照重复引用时的显示、stroke/currentColor 和页面交互。

**验证：**
- [ ] V-web；使用本地预览逐一对照原 SVG 的正常/hover/disabled 视觉。
- [ ] 检查重复实例 ID 和 clip 引用、编译输出与包体；新增预览工具只放 output，不加产品入口。

**依赖：** T37。

**预计修改文件：**
- `amoro-web/src/assets/icons/sprite.svg`

**范围：** S（同一个 sprite；验证产物不入提交）。

## T39：转换原生 SVG：Paimon 与操作图标

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 把 paimon、slide、tables、tag 四个本地素材转换为同源 sprite 中的 SVG symbol，原始素材保留。

**验收标准：**
- [ ] viewBox/path/颜色/尺寸/内部引用与原始素材一致，保留品牌/格式可辨识性与来源。
- [ ] 有局部 ID/clipPath 的素材采用每个 symbol 独立前缀并同步引用，无 v-html/DOMParser/新插件。
- [ ] 对照重复引用时的显示、stroke/currentColor 和页面交互。

**验证：**
- [ ] V-web；使用本地预览逐一对照原 SVG 的正常/hover/disabled 视觉。
- [ ] 检查重复实例 ID 和 clip 引用、编译输出与包体；新增预览工具只放 output，不加产品入口。

**依赖：** T38。

**预计修改文件：**
- `amoro-web/src/assets/icons/sprite.svg`

**范围：** S（同一个 sprite；验证产物不入提交）。

### 检查点 CP13：T37–T39

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T40：让 SvgIcon 使用同源 sprite

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 通过原生 SVG use 将既有 SvgIcon 接口接到 16 个本地 symbol，核对全站调用。

**验收标准：**
- [ ] 原 iconClass/className/isStroke/disabled/click/hover、类与颜色/尺寸语义不变，未知名称不注入外部内容。
- [ ] 16 名称映射完整，素材只由本地构建加载，无重复 ID/品牌失真或误点击。
- [ ] 未全量注册外部图标库，重复实例/菜单/表格式图标和卸载通过。

**验证：**
- [ ] V-web；在真实 AMS 回放所有菜单/表格式/branch/tag 图标与 hover/disabled。
- [ ] 用 svg-preview 与页面 DOM 核对重复实例、viewBox/引用，比较候选模块和资源成本。

**依赖：** T39。

**预计修改文件：**
- `amoro-web/src/components/svg-icon.vue`
- `amoro-web/src/assets/icons/sprite.svg`（替代旧注册入口）

**范围：** S（2 个文件；验证产物不入提交）。

## T41：移除 SVG 插件链

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 在新 SvgIcon 完整工作后移除旧插件、虚拟入口、类型和专属供给补丁。

**验收标准：**
- [ ] 没有 virtual:svg-icons-register/client 类型/旧插件 import 或仍使用的入口。
- [ ] 由 pnpm 删除插件并生成锁，移除仅供它使用的 fast-glob packageExtension，不误删其他依赖。
- [ ] 正式构建/dev/mock 均可加载本地图标，未修改业务 SVG 或运行配置。

**验证：**
- [ ] 严格 frozen 安装、V-web；启动 dev/dev:mock 做图标辅助检查。
- [ ] rg 和模块图核对 vite-plugin-svg-icons/virtual 入口与补丁；真实 AMS 图标仍完整。

**依赖：** T40。

**预计修改文件：**
- `amoro-web/src/main.ts`
- `amoro-web/src/vite-env.d.ts`
- `amoro-web/vite.config.mts`
- `amoro-web/package.json`
- `amoro-web/pnpm-lock.yaml`

**范围：** M（5 个文件；验证产物不入提交）。

## T42：收敛旧样式覆盖

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 删除已迁移的 Ant Design 全局覆盖，完成主题与 reset 顺序校正。

**验收标准：**
- [ ] 已迁移页面不依赖 .ant-* 内部 DOM，业务布局类仅作必要等效适配。
- [ ] Element Plus 主题与现有 Less/reset 的作用域/顺序明确，侧栏、图表与 Swagger 不受破坏。
- [ ] 所有表格/表单/浮层/空态/loading 在两视口和中英文无新增错乱。

**验证：**
- [ ] V-web；rg src 中残留 .ant-/anticon，逐条解释或修复，不做无关字符串清理。
- [ ] 真实 AMS 运行代表路径/Swagger 的截图与浮层定位回放。

**依赖：** T41。

**预计修改文件：**
- `amoro-web/src/styles/ant-design.less`
- `amoro-web/src/styles/index.less`
- `amoro-web/src/styles/element-plus.less`
- `amoro-web/src/styles/reset.less`

**范围：** M（4 个文件；验证产物不入提交）。

### 检查点 CP14：T40–T42

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T43：移除旧 UI 与复制依赖

**本地证据。** 严格安装、锁文件、Vite 模块清单和最终 JAR 均已核查，四个旧包退出。依赖链仍使用原有 Vue、Day.js、ECharts 和 Swagger UI。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 删除旧 Ant Design、旧图标、旧 clipboard 直接声明与无用途 resolver/chunk 规则。

**验收标准：**
- [ ] 由 pnpm 生成锁，旧三个包无剩余直接/传递或分发路径；前一步 SVG 旧包也仍不存在。
- [ ] 移除 App 旧 provider/locale；旧类型、调用、模板、图标无剩余；干净生成声明无旧包，保留基础依赖。
- [ ] 移除旧 resolver 与 antd 组，记录实际加载模块；更新技术栈说明但不编造性能结果。

**验证：**
- [ ] 严格 frozen 安装、V-web；pnpm why 与产物模块库存交叉检查四个旧包。
- [ ] 所有现存 src 模板/类型/样式/图标命中与 T01 库存闭环，生成声明无旧包。

**依赖：** T42。

**预计修改文件：**
- `amoro-web/package.json`
- `amoro-web/pnpm-lock.yaml`
- `amoro-web/vite.config.mts`
- `amoro-web/README.md`
- `amoro-web/src/App.vue`

**范围：** M（5 个文件；验证产物不入提交）。

## T44：清理退出分发的许可材料

**本地证据。** 已根据候选产物清单移除四份旧直接许可，原有 Swagger 嵌入归属保留。

- [x] **任务完成**（验收与验证通过后勾选）。

**说明：** 仅移除四个确认不再使用的旧包独立许可，保留仍分发模块的材料。

**验收标准：**
- [x] 删除旧 UI、旧图标、clipboard、SVG 插件四个材料前均有依赖/模块图证明。
- [x] 未顺手清空整个 licenses 或删掉 Swagger 嵌入模块/仍需归属的版权。
- [x] 许可差异与旧包清理一一对应，历史无关材料不混入本次批量处理。

**验证：**
- [x] 复核 T43 依赖/模块图及四份许可来源，查看 git diff。
- [x] 确认后续 T45 会以新产物重建总 NOTICE，当前增量有明确接续。

**依赖：** T43。

**预计修改文件：**
- `amoro-web/licenses/LICENSE-ant-design-vue`
- `amoro-web/licenses/LICENSE-@ant-design-icons-vue`
- `amoro-web/licenses/LICENSE-vue-clipboard3`
- `amoro-web/licenses/LICENSE-vite-plugin-svg-icons`

**范围：** M（4 个文件；验证产物不入提交）。

## T45：更新实际分发 NOTICE

**本地证据。** 产物清单 132 个 Vite 包及 13 个 Swagger 嵌入归属均有 NOTICE 条目。发行 JAR 已核对 NOTICE 字节。审计基线 14 项，候选 9 项，候选 ID 均出现在基线。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 以候选模块、精确包与 Swagger 嵌入图核对新增许可和总 NOTICE。

**验收标准：**
- [ ] 为 Element Plus `2.14.8`、图标 `2.3.2` 和产物实际包含的传递包（含实际解析的 `@ctrl/tinycolor`）核对许可、版权及声明，并覆盖已有 Swagger 嵌入包。
- [ ] THIRD-PARTY-NOTICES 与新输出一致；审计新旧路径/严重级别，既有问题分列。
- [ ] 若额外版权文件使本项超过 5 文件，先展开小子任务；不能漏材料或用 gen:license 代替核对。

**验证：**
- [ ] 核对所有分发模块到包/精确版本/来源文本；pnpm audit --prod --json 并比较 B0。
- [ ] 记录 NOTICE hash 与材料清单，验证 Maven frontend-licenses 仍打包完整。

**依赖：** T44。

**预计修改文件：**
- `amoro-web/licenses/LICENSE-element-plus`
- `amoro-web/licenses/LICENSE-@element-plus-icons-vue`
- `amoro-web/licenses/THIRD-PARTY-NOTICES`
- `output/element-plus-verification/distribution/license-inventory.json`

**范围：** M（4 个文件；验证产物不入提交）。

### 检查点 CP15：T43–T45

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T46：完成候选自动检查

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 在旧依赖已退出的候选上重跑可复现安装、全部既有与新增检查。

**验收标准：**
- [ ] 严格 frozen 安装、两项类型检查、既有及新增必要测试/正式构建通过，检查未降级。
- [ ] lint 相对 B0 无新增未解决诊断，audit 新增路径闭环，dev/mock 辅助路径不回归。
- [ ] 组件/模块/依赖库存闭合；无第二份 Vue/Day.js、全量 UI 注册或无关路由资源加载。

**验证：**
- [ ] 执行 Plan §7 全部前端命令及 ui-feedback/clipboard/采集分析自测，保存退出码。
- [ ] 审阅模块图、冷启动资源清单和锁文件，复核 UI/图标精确版本、单一 Vue/图标依赖及四个旧包的退出情况。

**依赖：** T45。

**预计修改文件：**
- `output/element-plus-verification/candidate/checks.json`
- `output/element-plus-verification/candidate/module-inventory.json`

**范围：** S（2 个文件；验证产物不入提交）。

## T47：验证正式 AMS 产物身份

**本地证据。** 基线与候选的 Vite 资源、Maven JAR、发行包和 HTTP 内容一致，详细哈希见 `output/element-plus-verification/artifact-identity-visual-final.json`。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 清理旧前端输出后组装分发，在独立本地 AMS 加载候选与 B0。

**验收标准：**
- [ ] Vite static、JAR、发行包、实际 HTTP JS/CSS hash 相符，没有旧哈希残留或 mock 响应。
- [ ] B0/M 后端、JDK/参数/fixture 条件相同，记录 commit/PID/端口/工具，真实登录与 API 正确。
- [ ] 许可已包含在发行产物；所有正式验收使用该身份，不把脚本启动成功当运行通过。

**验证：**
- [ ] 执行 V-dist/V-ams；验证 HTTP 资源、JAR 和当前源码/模块库存。
- [ ] 用独立端口和非默认测试管理员密码运行同源 UI 冒烟，恢复 fixture，保留脱敏的日志与身份清单。

**依赖：** T46。

**预计修改文件：**
- `output/element-plus-verification/distribution/artifact-identity.json`
- `output/element-plus-verification/candidate/runtime-identity.json`

**范围：** S（2 个文件；验证产物不入提交）。

## T48：验收身份与管理功能

**本地证据。** 真实 AMS 已覆盖管理员登录、各主入口、资源组创建及确认取消/删除、Catalog 空表单拦截和 XML 上传/下载。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 在最终 AMS 产物上重放身份导航、Catalog/数据库、资源管理和公共页面。

**验收标准：**
- [ ] T04/T06 功能对照逐项通过，真实写操作成功/取消/失败/权限与刷新结果一致。
- [ ] Overview/Settings/Swagger/404/语言及图表覆盖完整，未因旧组件退出漏功能。
- [ ] Console/Network 无新增未解决异常/双请求/资源失败，缺项明确待验收。

**验证：**
- [ ] 运行 identity-catalog/management 回放；实际核对 API 与持久状态。
- [ ] 对照 B0 操作证据和旧问题清单，失败先回溯相应小任务修复并复测。

**依赖：** T47。

**预计修改文件：**
- `output/element-plus-verification/parity.json`
- `output/element-plus-verification/candidate/management-acceptance.json`

**范围：** S（2 个文件；验证产物不入提交）。

### 检查点 CP16：T46–T48

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T49：验收多格式表功能

**本地证据。** 真实 AMS 已覆盖千表目录、刷新后的表路由、Paimon 快照引用与展开、Tag 写入、Iceberg 操作详情复制。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 在最终 AMS 上重放表入口、所有格式详情/快照/Tag/Hive/优化路径。

**验收标准：**
- [ ] T05 对照逐项通过，bounded/legacy/branch/tag/consumer、Files/health/TableModel 无遗漏。
- [ ] Tag 和 Hive/优化等写操作、只读/权限/能力、过期/重复提交/失败结果均有真实证据。
- [ ] query/选中/分页/历史、数据精度/时间/单位及 ≥1,000 节点访问与 B0 一致。

**验证：**
- [ ] 运行 tables 回放和真实写入后刷新，覆盖各格式/权限 fixture。
- [ ] 核对全部功能 ID 结论、请求计数/字段、Console 与截图，不用一种格式推断其他格式。

**依赖：** T48。

**预计修改文件：**
- `output/element-plus-verification/parity.json`
- `output/element-plus-verification/candidate/tables-acceptance.json`

**范围：** S（2 个文件；验证产物不入提交）。

## T50：验收视觉与输入兼容

**本地证据。** 在 Chrome 1440×900 和 1280×720 对照旧版与最终候选的中英文截图。修复了概览卡片间距、侧栏选中项、快照表头和展开区、优化页数值提示截断和空态文案。随后在五种宽度对照了 155 对页面状态，并通过 390×520 的五条触摸流程和两宽度的中文切换复测。其他浏览器与角色组合按本清单继续验收。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 完成最终中英文、两视口、支持浏览器、键盘/焦点及普通 HTTP 复制回归。

**验收标准：**
- [ ] 1440×900/1280×720、中英文与空态/loading/错误/长文本无布局错乱或信息丢失。
- [ ] Tab/Enter/Escape、中文输入法、图标和可访问名称正常。T10、T11、T14、T19、T24、T25 的新版用例在正式 AMS 上复测通过，覆盖菜单、动态禁用、Dialog 和表头。
- [ ] 普通非安全 HTTP 与 localhost 的实际剪贴板、平台拒绝/回退/失败正确，不以 toast 替代文本核对。

**验证：**
- [ ] 在真实浏览器执行视觉/输入矩阵，对比 B0 的同视口、同数据和同语言截图。核验概览卡片间距、优化页提示宽度、快照表头和展开区背景及中英文文本。
- [ ] 支持引擎功能回归与真实剪贴板核查；记录 UA/viewport/locale/安全上下文及限制。

**依赖：** T49。

**预计修改文件：**
- `output/element-plus-verification/candidate/visual-input-acceptance.json`
- `output/element-plus-verification/parity.json`

**范围：** S（2 个文件；验证产物不入提交）。

## T51：验证性能收益与重复操作稳定性

**本地证据。** 10 对有效样本。响应式交付复测中 P1 中位数降低 34.8%，P2 降低 59.3%，均超过各自的 A/A 噪声，95% 区间高于零。千表展开的绘制调度中位数由 278.2 ms 降至 185.7 ms。详见本地验证记录。

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 对最终 B0/M 正式产物交错测量 P1–P5，证明两项 10% 和无可复现退化。

**验收标准：**
- [x] P1/P2 每端 ≥10 有效配对样本，均 ≥10% 改善且超过 A/A 噪声，差异 95% CI 不跨 0。
- [ ] 其他操作 p50/p95、包/模块、CPU/style/layout/paint/长任务/LCP/CLS 均解释，无确认的新增退化。
- [ ] ≥10 轮往返/弹窗与 GC/DOM/监听/实例/内存观察无新增泄漏证据；原始 trace 和全部样本齐全。

**验证：**
- [ ] 执行 Plan §7 paired 采样/分析接口，复核固定合同/工具 hash、CI 和所有无效样本理由。
- [ ] 串行重放 P5 稳定性；未达标按 trace 新增小修复任务，再跑相关验证与配对采样，不降阈值。

**依赖：** T50、T07。

**预计修改文件：**
- `output/element-plus-verification/paired/summary.json`
- `output/element-plus-verification/paired/stability.json`
- `output/element-plus-verification/report.md`

**范围：** M（3 个文件；验证产物不入提交）。

### 检查点 CP17：T49–T51

- [ ] 本组任务的验收、验证均完成；源码组 V-web 通过，文档/数据准备组对应工具/运行验证通过。
- [ ] 相关用户路径仍可操作，无未解决新增问题；准备组满足进入下一阶段的前提。
- [ ] 记录 commit、命令退出码、功能 ID、API/截图/trace 与限制，供人工审阅；未通过不推进。

## T52：审查并汇总交付证据

- [ ] **任务完成**（验收与验证通过后勾选）。

**说明：** 将功能、依赖、许可、构建、真实运行和性能证据逐条映射到 Spec AC，形成可审阅结果。

**验收标准：**
- [ ] AC-01–AC-10 都有明确通过/待验收及证据；没有未验证项被描述为完成。
- [ ] 审查功能/权限/API 不变量、复杂度/维护性、输入/资源安全、性能归因、许可和回退，发现问题已闭环。
- [ ] 报告和任务状态一致，工作区只含相关变更，无凭据/DB/日志/构建物；保留 B0 回退方式。

**验证：**
- [ ] 审阅 git diff、锁文件/模块图、报告与全部检查点，复核关键证据可打开。
- [ ] 未满足任一合同继续保持待验收；提交审阅但不擅自 push/建 PR/合并/部署。

**依赖：** T51。

**预计修改文件：**
- `tasks/element-plus-migration-report.md`
- `tasks/todo.md`
- `tasks/plan.md`
- `SPEC-element-plus-migration.md`

**范围：** M（4 个文件；验证产物不入提交）。

### 最终审阅检查点：T52

- [ ] 全部任务与 Spec AC-01–AC-10 的证据一致，功能不变量、两项 ≥10% 与无退化同时成立。
- [ ] 许可证/审计/产物身份、遗留风险和回退材料齐全，无运行数据/敏感材料入提交。
- [ ] 用户已审阅交付结果；后续 push、PR、合并与部署按明确指令执行。
