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

# Swift BDD runner: builds the Swift SDK and the runner, which parses the
# mounted feature files and runs their scenarios on the SDK. The toolchain
# matches .github/actions/utils/setup-swift.
FROM swift:6.4.0

WORKDIR /app
COPY foreign/swift ./foreign/swift
COPY bdd/swift ./bdd/swift
COPY bdd/scenarios/deferred_polling.feature ./features/

RUN cd bdd/swift/runner && swift build --product iggy-bdd

CMD ["/app/bdd/swift/runner/.build/debug/iggy-bdd", "/app/features"]
