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

# The Swift version and Ubuntu release the Linux CI lanes install.
FROM swift:6.4.0-resolute

WORKDIR /app

# The SDK is a path dependency of the runner, so it keeps its place
# relative to the runner as in the repository.
COPY foreign/swift ./foreign/swift
COPY bdd/swift/tests/Package.swift bdd/swift/tests/Package.resolved ./bdd/swift/tests/

WORKDIR /app/bdd/swift/tests
RUN swift package resolve

COPY bdd/swift/tests/Sources ./Sources
COPY bdd/scenarios /app/bdd/scenarios
RUN swift build -Xswiftc -warnings-as-errors

CMD ["swift", "run", "--skip-build", "iggy-bdd"]
