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

"""Validate the packaged runtime without starting Spark or deleting data."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import subprocess
import tempfile
import xml.etree.ElementTree as ET

parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument("version", choices=("0.9.0", "1.3.2"))
args = parser.parse_args()
folder = Path(__file__).resolve().parent
root = folder.parent.parent
jar = root / "amoro-producer/target/amoro-producer-1.0.0-jar-with-dependencies.jar"
report = next((root / "amoro-producer/target/surefire-reports").glob("TEST-*.xml"))
properties = ET.parse(report).getroot().find("properties")
classpath = next(item.attrib["value"] for item in properties
                 if item.attrib["name"] == "java.class.path")
java_bin = Path(os.environ["JAVA_HOME"]) / "bin" if "JAVA_HOME" in os.environ else None

def command(name):
    return str(java_bin / name) if java_bin else name

with tempfile.TemporaryDirectory(prefix="producer-runtime-check-") as temporary:
    subprocess.run([command("javac"), "-d", temporary,
                    str(folder / "SnapshotPackagedRuntimeCheck.java")], check=True)
    result = subprocess.run(
        [command("java"), "-cp", os.pathsep.join((str(jar), temporary, classpath)),
         "SnapshotPackagedRuntimeCheck", str(jar), args.version],
        check=True, capture_output=True, text=True)
    print(result.stdout, end="")
    print(result.stderr, end="")
    evidence = {
        "paimon": args.version, "jar": jar.name, "bytes": jar.stat().st_size,
        "sha256": hashlib.sha256(jar.read_bytes()).hexdigest(),
        "exitCode": result.returncode, "output": result.stdout.strip(),
    }
    (folder / ("package-" + args.version + ".json")).write_text(
        json.dumps(evidence, indent=2) + "\n")
