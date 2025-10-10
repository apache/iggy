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

FROM node:lts-alpine AS deps
WORKDIR /app
COPY package*.json ./
RUN npm ci --prefer-offline --no-audit

FROM node:lts-alpine AS builder
WORKDIR /app

COPY --from=deps /app/node_modules ./node_modules
COPY . .

RUN npm run build
FROM node:lts-alpine AS production

WORKDIR /app

COPY package*.json ./
RUN npm ci --production --prefer-offline --no-audit

COPY --from=builder /app/build ./build
USER node

EXPOSE 3050

ENV PORT=3050
ENV NODE_ENV=production

CMD ["node", "build"]
