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

# The conformance agent of the Swift SDK. Build it from the repository root:
#   docker build -f conformance/agents/swift/Dockerfile -t iggy-conformance-swift .
# The agent speaks the protocol on stdin and stdout, so the orchestrator
# starts it with:
#   docker run -i --rm --network host iggy-conformance-swift

# The Swift version and Ubuntu release of the BDD image and the Linux CI lanes.
FROM swift:6.4.0-resolute

WORKDIR /app

# The SDK is a path dependency of the agent, so it keeps its place relative
# to the agent as in the repository.
COPY foreign/swift ./foreign/swift
COPY conformance/agents/swift/agent/Package.swift conformance/agents/swift/agent/Package.resolved ./conformance/agents/swift/agent/

WORKDIR /app/conformance/agents/swift/agent
RUN swift package resolve

COPY conformance/agents/swift/agent/Sources ./Sources
RUN swift build -Xswiftc -warnings-as-errors \
    && cp "$(swift build --show-bin-path)/iggy-agent" /usr/local/bin/iggy-agent

CMD ["iggy-agent"]
