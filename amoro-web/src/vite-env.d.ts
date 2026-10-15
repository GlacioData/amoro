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

/// <reference types="vite/client" />
// v2.0.1 ships client.d.ts but its exports map does not expose the client subpath.
// Load the upstream declaration directly while keeping bundler resolution strict.
// Source: https://www.typescriptlang.org/docs/handbook/triple-slash-directives.html#-reference-path-
/// <reference path="../node_modules/vite-plugin-svg-icons/client.d.ts" />

// ECharts supplies this side-effect theme registration as JavaScript without declarations.
// Source: https://echarts.apache.org/handbook/en/basics/release-note/v6-upgrade-guide/
declare module 'echarts/theme/v5' {}
