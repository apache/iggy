#!/bin/bash
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


# Change to the directory where your E2E tests are located
cd "$(dirname "$0")/../e2e/tcp_test" || exit

# Assuming you have 'ginkgo' installed, you can use it to run your Ginkgo tests
# -r randomizes the order of the tests
ginkgo -r

# Optionally, you can also add commands to perform post-test actions or cleanup here
# For example, stopping services or generating test reports
