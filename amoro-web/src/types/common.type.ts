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

export interface IOption {
  [propName: string]: any
}
export interface IHttpResponse {
  code: number
  msg: string
  result?: IOption
  data?: IOption
}
export interface UserInfo {
  userName: string
  role?: string
  roles?: string[]
  privileges?: string[]
  token?: string
}

export interface IColumns {
  title: string
  dataIndex: string
  key?: string
  ellipsis?: boolean
  width?: number | string
  scopedSlots?: any
  children?: IColumns[]
}

export interface IOptions {
  label: string
  value: string
  [propName: string]: string
}

export interface ILableAndValue {
  label: string
  value: string
}
export interface IMap<T> {
  [key: string]: T
}

export interface IKeyAndValue {
  key: string
  value: string
}

export interface TableHealthComponent {
  code: string
  score: number
  weight: number | null
  combination: string | null
  metrics: Record<string, string>
}

export interface TableHealthBand {
  code: string
  lowerBound: string | null
  upperBound: string | null
  fileCount: string
  fileSize: string
  sortedRuns: string | null
}

export interface TableHealthProblem {
  partition: string
  bucket: number | null
  code: string
  severity: string
  fileCount: string
  fileSize: string
  sortedRuns: string
  bands: TableHealthBand[]
}

export interface TableHealthStructure {
  tableShape: string
  availability: string
  evaluatedAtMillis: number
  snapshotTimeMillis: number
  totalUnits: string
  affectedUnits: string
  severeUnits: string
  bands: TableHealthBand[]
  problems: TableHealthProblem[]
}

export interface TableHealthEvaluation {
  status: string
  stale: boolean
  currentSnapshotId: string | null
  attemptedSnapshotId: string | null
  attemptedAtMillis: number
  retryable: boolean
  reasonCodes: string[]
}

export interface TableFileStatistics {
  status: string
  source: string
  snapshotId: string | null
  schemaId: string | null
  observedSnapshotId: string | null
  observedSchemaId: string | null
  fileCount: string | null
  totalSizeBytes: string | null
  averageFileSizeBytes: string | null
  evaluatedAtMillis: number | null
  collectionEnabled: boolean | null
  reasonCodes: string[]
}

export interface TableHealthDetails {
  structure?: TableHealthStructure | null
  evaluation?: TableHealthEvaluation | null
  formulaVersion: string
  snapshotId: string | null
  changeSnapshotId: string | null
  schemaId: string | null
  scoringConfigFingerprint: string
  evaluationKey: string
  components: TableHealthComponent[]
  metrics: Record<string, string>
  reasonCodes: string[]
}

export interface TableHealthComparison {
  processId: string
  optimizingType: string | null
  status: string
  reason: string | null
  concurrentChanges: boolean
  beforeScore: number
  afterScore: number
  before: TableHealthDetails | null
  after: TableHealthDetails | null
  completedAtMillis: number
}

export interface ITableModel {
  kind: string
  code: string
}

export interface IBaseDetailInfo {
  boundedSnapshotRead: boolean | null
  optimizingStatus: string
  records: string
  tableType: string
  tableName: string
  createTime: string
  tableModel: ITableModel | null
  size: string
  file: string | number | null
  averageFile: string
  tableFormat: string
  hasPartition: boolean
  hasPrimaryKey: boolean
  healthScore: number
  smallFileScore: number
  equalityDeleteScore: number
  positionalDeleteScore: number
  healthDetails: TableHealthDetails | null
  healthComparison?: TableHealthComparison | null
  comment: string
}

export interface ResourceUsageItem {
  ts: number
  totalCpu: number
  totalMemory: number
}

export interface DataSizeItem {
  ts: string
  dataSize: number
}

export interface ITopTableItem {
  tableName: string
  tableSize: number
  fileCount: number
  averageFileSize: number
  healthScore: number
}

export interface DetailColumnItem {
  checked?: unknown
  field: string
  type: string
  required: boolean
  comment: string
}

export interface IField {
  field: string
  type: string
  required: boolean
  description: string
}

export interface PartitionColumnItem {
  field: string
  sourceField: string
  transform: string
}

export interface IDetailsInfo {
  pkList: DetailColumnItem[]
  partitionColumnList: PartitionColumnItem[]
  properties: IMap<string>[]
  metrics: IMap<string | number>[]
  schema: DetailColumnItem[]
}
export interface ICompMap {
  Details: string
  Partitions: string
  Snapshots: string
  Operations: string
  Optimizes: string
}
export interface TableBasicInfo {
  catalog: string
  database: string
  tableName: string
}

export interface PartitionItem {
  partition: string
  fileCount: number
  size: string
  lastCommitTime: number | string
  specId: number
}

export interface BreadcrumbPartitionItem {
  file: string
  fsn: number
  fileType: string
  size: string
  commitTime: number | string
  commitId: string | null
  path: string
}

export interface BreadcrumbSnapshotItem {
  file: string
  fsn: number
  partition: string
  fileType: string
  size: string
  commitTime: number | string
}

export interface FilesStatistics {
  fileCnt: number
  totalSize: number
  averageSize: number
}

export interface BreadcrumbOptimizingItem {
  tableId: number
  processId: string
  taskId: number
  partitionData: string
  status: string
  retryNum: number
  optimizerToken: string
  threadId: number
  thread: string
  startTime: number | string
  endTime: number | string
  costTime: number
  formatCostTime: string
  failReason: string
  summary: Record<string, string>
  inputFiles: FilesStatistics
  outputFiles: FilesStatistics
  inputFilesDesc: string
  outputFilesDesc: string
  properties: Record<string, string>
}

// AmoroSnapshotsOfTable uses Map<String, String>, with format-specific keys.
export type SnapshotItemSummary = Record<string, string | null>

export interface SnapshotItem {
  snapshotId: string
  operation: string
  producer: string
  fileCount: number | null
  records: number
  commitTime: string
  summary: SnapshotItemSummary
  filesSummaryForChart: SnapshotItemSummary | null
  recordsSummaryForChart: SnapshotItemSummary | null
}

export interface OverviwOperationItem {
  tableName: string
  tableIdentifier: ITableIdentifier
  operation: string
  ts: number | string
}

export interface OperationItem {
  ts: number | string
  operation: string
}

export interface IHistoryPathInfoItem {
  path: string
  query: IOption
}

export interface GlobalState {
  userInfo: UserInfo
  isShowTablesMenu: boolean
  historyPathInfo: IHistoryPathInfoItem
}

export interface ICatalogItem {
  catalogName: string
  catalogType: string
}
export interface IDebugResult {
  status: string
  columns: string[]
  rowData: (string | null)[][]
  id: string
}

export interface IGroupItem {
  optimizerGroupId: number
  optimizerGroupName: string
  label: string
  value: string
}

export interface IGroupItemInfo {
  occupationCore: number
  occupationMemory: number | string
  unit: string
}

export interface ITableIdentifier {
  catalog: string
  database: string
  tableName: string
  id: number
}
export interface IOptimizeTableItem {
  tableName: string
  groupName: string
  healthScore: number
  optimizeStatus: string
  fileCount: number
  fileSize: number
  quota: number
  quotaOccupation: number
  quotaOccupationDesc: string
  duration: number
  durationDesc: string
  fileSizeDesc: string
  tableIdentifier: ITableIdentifier
  tableNameOnly?: string
}

export interface IIOptimizeGroupItem {
  resourceGroup: {
    name: string
    container: string
    properties: { [prop: string]: string }
  }
  occupationCore: number
  occupationMemory: number
  name: string
  container: string
  resourceOccupation: string
}

export interface IOptimizeResourceTableItem {
  touchTime: string
  startTime: string
  index: number
  jobId: number
  jobStatus: string
  coreNumber: number
  memory: number
  jobmanagerUrl: string
  parallelism: number
  jobType: string
  groupName: string
  resourceAllocation: string
}

export interface IOverviewSummary {
  catalogCnt: number
  tableCnt: number
  tableTotalSize: number
  totalCpu: string
  totalMemory: number
  displayTotalSize?: string
  displayTotalMemory?: string
  totalSizeUnit?: string
  totalMemoryUnit?: string
}
export interface ITimeInfo {
  yTitle: string
  colors: string[]
  name: string[]
}

export interface IChartLineData {
  timeLine: string[]
  data1: number[] | string[]
  data2: number[] | string[]
}

export interface IOptimizeGroup {
  container: string
  name: string
  properties: IMap<any>
  innerPropertiesArray?: any[]
}

export interface IContainerSetting {
  name: string
  classpath: string
  properties: IMap<string>
  optimizeGroup: IOptimizeGroup[]
  propertiesArray?: IMap<any>[]
}

export interface IResourceUsage {
  timeLine: string[]
  usedCpu: string[]
  usedCpuDivision: string[]
  usedCpuPercent: string[]
  usedMem: string[]
  usedMemDivision: string[]
  usedMemPercent: string[]
}

export enum debugResultBgcMap {
  Created = '#f5f5f5',
  Failed = '#fff2f0',
  Finished = '#f6ffed',
  // eslint-disable-next-line ts/no-duplicate-enum-values
  Canceled = '#f5f5f5',
}

export enum upgradeStatusMap {
  failed = 'FAILED',
  upgrading = 'UPGRADING',
  success = 'SUCCESS',
  none = 'NONE', // can upgrade
}

export enum tableTypeIconMap {
  ICEBERG = 'iceberg',
  ARCTIC = 'amoro',
  HIVE = 'hive',
  PAIMON = 'paimon',
}

export type ILineChartOriginalData = Record<string, Record<string, number | null>>

export enum branchTypeMap {
  BRANCH = 'branch',
  TAG = 'tag',
  CONSUMER = 'consumer',
}

export interface IBranchItem {
  value: string
  label: string
  type: branchTypeMap
  amoroCurrentSnapshotsOfTable?: SnapshotItem | null
}

export interface IServiceBranchItem {
  name: string
  snapshotId: number
  minSnapshotsToKeep: number | null
  maxSnapshotAgeMs: number | null
  maxRefAgeMs: number | null
  type: branchTypeMap
  consumerId: string
  amoroCurrentSnapshotsOfTable: SnapshotItem | null
}

export enum operationMap {
  ALL = 'all',
  OPTIMIZING = 'optimizing',
  NONOPTIMIZING = 'non-optimizing',
}
