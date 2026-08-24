-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- One homogeneous KV table carries the whole Process domain: each row stores one Process
-- as a Base64-encoded YAML document in `value` (a 64KiB document encodes to ~87KB, which is
-- why the column is MEDIUMTEXT). Framework-generic domains that opt in own their table
-- creation; only this table ships.

CREATE TABLE IF NOT EXISTS amoro_process_v2 (
  name         VARCHAR(255) NOT NULL,
  collection   VARCHAR(255) NOT NULL,
  value        MEDIUMTEXT   NOT NULL,
  last_updated DATETIME     NOT NULL,
  PRIMARY KEY (name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;
