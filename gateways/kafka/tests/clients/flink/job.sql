-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--   http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- The Flink Kafka connector's SQL tables over the matrix topics. client_matrix_tests.rs replaces
-- the @NAME@ placeholders. The source reads from the earliest offset to the latest at start
-- (bounded), so the INSERT finishes; table.dml-sync makes the SQL client wait for it.
SET 'table.dml-sync' = 'true';
SET 'execution.runtime-mode' = 'streaming';
SET 'sql-client.execution.result-mode' = 'tableau';

CREATE TABLE matrix_source (
    k STRING,
    i INT,
    v STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '@SOURCE@',
    'properties.bootstrap.servers' = '@BOOTSTRAP@',
    'properties.group.id' = '@GROUP@',
    'scan.startup.mode' = 'earliest-offset',
    'scan.bounded.mode' = 'latest-offset',
    'key.format' = 'raw',
    'key.fields' = 'k',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY'
);

CREATE TABLE matrix_sink (
    k STRING,
    i INT,
    v STRING
) WITH (
    'connector' = 'kafka',
    'topic' = '@SINK@',
    'properties.bootstrap.servers' = '@BOOTSTRAP@',
    'sink.delivery-guarantee' = 'at-least-once',
    'key.format' = 'raw',
    'key.fields' = 'k',
    'value.format' = 'json',
    'value.fields-include' = 'EXCEPT_KEY'
);

INSERT INTO matrix_sink SELECT k, i, v FROM matrix_source;
