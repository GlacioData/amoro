#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
#
"""Rebuild the old-API binary fixture using JDK 8 and the fixed baseline source."""
import os
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[5]
BASE = Path(__file__).resolve().parent
SHA = "d5685bf73786ab2cde1ba5062ce0a08a183f159c"
JDK = Path(os.environ.get("JAVA_HOME", "/Library/Java/JavaVirtualMachines/zulu-8.jdk/Contents/Home"))
report = ROOT / "amoro-format-paimon/target/surefire-reports/TEST-org.apache.amoro.formats.paimon.optimizing.primary.TestPaimonPrimaryKeyTableCommit.xml"
props = {p.attrib["name"]: p.attrib["value"] for p in ET.parse(report).findall("./properties/property")}
cp = props["java.class.path"]
with tempfile.TemporaryDirectory(prefix="jspt2717-legacy-") as temp:
    temp = Path(temp)
    sources = []
    for module, relative in [
        ("amoro-common", "org/apache/amoro/optimizing/TableOptimizingCommitter.java"),
        ("amoro-common", "org/apache/amoro/optimizing/OptimizationContext.java"),
        ("amoro-format-paimon", "org/apache/amoro/formats/paimon/optimizing/primary/PaimonPrimaryKeyCompactionOutput.java"),
    ]:
        source = temp / relative
        source.parent.mkdir(parents=True, exist_ok=True)
        source.write_bytes(subprocess.check_output(["git", "show", f"{SHA}:{module}/src/main/java/{relative}"], cwd=ROOT))
        sources.append(str(source))
    out = temp / "classes"
    out.mkdir()
    subprocess.run([str(JDK / "bin/javac"), "-source", "8", "-target", "8", "-cp", cp, "-d", str(out), *sources, str(BASE / "LegacyPlugin.java"), str(BASE / "WriteLegacyOutput.java")], check=True)
    subprocess.run([str(JDK / "bin/java"), "-cp", str(out) + os.pathsep + cp, "WriteLegacyOutput", str(BASE / "output.bin")], check=True)
    subprocess.run([str(JDK / "bin/jar"), "cf", str(BASE / "plugin.jar"), "-C", str(out), "org/apache/amoro/compat/LegacyPlugin.class"], check=True)
    subprocess.run([str(JDK / "bin/javap"), "-verbose", str(out / "org/apache/amoro/compat/LegacyPlugin.class")], check=True)
