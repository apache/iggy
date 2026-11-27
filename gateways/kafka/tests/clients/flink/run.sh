#!/bin/sh
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# One Flink session cluster in this container, then one bounded SQL job (job.sql) that copies the
# matrix's records from a source topic to a sink topic with KafkaSource and an AT_LEAST_ONCE
# KafkaSink. The cluster binds loopback on the ports the test chose (REST_PORT, RPC_PORT) so the
# container can run on the host network next to the brokers.
set -eu
conf=/opt/flink/conf/config.yaml
{
    echo "rest.port: ${REST_PORT}"
    echo "rest.bind-address: 127.0.0.1"
    echo "rest.address: 127.0.0.1"
    echo "jobmanager.rpc.port: ${RPC_PORT}"
    echo "jobmanager.rpc.address: 127.0.0.1"
    echo "jobmanager.bind-host: 127.0.0.1"
    echo "taskmanager.bind-host: 127.0.0.1"
    echo "taskmanager.host: 127.0.0.1"
    echo "taskmanager.numberOfTaskSlots: 1"
    echo "taskmanager.memory.process.size: 1024m"
    echo "jobmanager.memory.process.size: 768m"
    echo "parallelism.default: 1"
} >> "$conf"
/opt/flink/bin/start-cluster.sh >/dev/null
/opt/flink/bin/sql-client.sh -f /job/job.sql
