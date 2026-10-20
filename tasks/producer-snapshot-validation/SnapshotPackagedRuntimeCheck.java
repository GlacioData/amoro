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

import java.io.File;
import java.lang.reflect.InvocationTargetException;

public class SnapshotPackagedRuntimeCheck {
  public static void main(String[] args) throws Exception {
    Class<?> table = Class.forName("org.apache.paimon.table.Table");
    File source = new File(table.getProtectionDomain().getCodeSource().getLocation().toURI());
    if (!source.getCanonicalPath().equals(new File(args[0]).getCanonicalPath())) {
      throw new AssertionError("Paimon did not load from the packaged artifact");
    }
    Object runtime = Class.forName("org.apache.amoro.producer.paimon.PaimonSnapshotRuntime$")
        .getField("MODULE$").get(null);
    Object version = runtime.getClass().getMethod("versionOf", Class.class, String.class)
        .invoke(runtime, table, "paimon-core");
    if (!args[1].equals(version)) throw new AssertionError("Wrong actual Paimon version: " + version);
    Class<?> confClass = Class.forName("org.apache.spark.SparkConf");
    Object conf = confClass.getConstructor(boolean.class).newInstance(false);
    confClass.getMethod("set", String.class, String.class).invoke(conf, "spark.task.maxFailures", "1");
    boolean accepted = false;
    try {
      runtime.getClass().getMethod("validate", String.class, confClass).invoke(runtime, "3.5.4", conf);
      accepted = true;
    } catch (InvocationTargetException error) {
      if (!error.getCause().getClass().getSimpleName().equals("ProducerRuntimeValidationException")) throw error;
    }
    if (accepted != "1.3.2".equals(args[1])) throw new AssertionError("Wrong runtime admission result");
    System.out.println("PACKAGED_RUNTIME paimon=" + version + " accepted=" + accepted + " source=" + source.getName());
  }
}
