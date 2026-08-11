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

package org.apache.amoro.utils;

/** Helpers for keeping health score values in the inclusive 0-100 range. */
public class ScoreUtil {

  private ScoreUtil() {}

  public static int clampScore(long score) {
    return (int) Math.max(0L, Math.min(100L, score));
  }

  public static double clampPercentage(double score) {
    if (Double.isNaN(score) || score < 0.0D) {
      return 0.0D;
    }
    return Math.min(score, 100.0D);
  }
}
