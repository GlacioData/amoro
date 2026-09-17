<!--
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing,
  software distributed under the License is distributed on an
  "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
  KIND, either express or implied.  See the License for the
  specific language governing permissions and limitations
  under the License.
-->

# 旧接口和旧序列化兼容夹具

基线：`d5685bf73786ab2cde1ba5062ce0a08a183f159c`（JSPT-2716）。`rebuild.py` 从该固定 Git 版本提取旧 `TableOptimizingCommitter`、`OptimizationContext` 和 `PaimonPrimaryKeyCompactionOutput` 源码，再使用 JDK 8 编译。

- `plugin.jar` 仅含基于旧接口编译的 `LegacyPlugin.class`，不包含接口类；测试时连接当前实现的默认方法。
- `output.bin` 由旧输出类序列化生成，缺少新增 skipReason 字段。
- 两个 Java 文件用于重建夹具，不属于生产代码。新测试不依赖运行时 javac 或 Git。

在仓库根、完成一次 `TestPaimonPrimaryKeyTableCommit` 后重建：

```bash
JAVA_HOME=/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home python3 amoro-format-paimon/src/test/resources/paimon-full-skip-legacy/rebuild.py
```

脚本只利用 Surefire 报告中记录的依赖 classpath；旧接口和输出类始终从固定基线读取，生成目录位于临时目录。二进制兼容不等于混合版本的业务语义兼容；发布顺序见 `tasks/paimon-f2-f7/configuration.md`。
