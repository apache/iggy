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

ARG RUST_VERSION=1.92
FROM rust:${RUST_VERSION}

FROM gradle:9.2-jdk17

WORKDIR /workspace
COPY . .

WORKDIR /workspace/bdd/java
RUN mkdir -p src/test/resources/features

# Create entrypoint script to copy mounted features at runtime
RUN echo '#!/bin/bash\n\
set -e\n\
if [ -d /app/features ]; then\n\
  cp -f /app/features/*.feature /workspace/bdd/java/src/test/resources/features/ 2>/dev/null || true\n\
fi\n\
exec "$@"' > /entrypoint.sh && chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
CMD ["gradle", "--no-daemon", "test"]
