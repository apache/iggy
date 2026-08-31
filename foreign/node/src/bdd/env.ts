// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// A default here would turn a dropped compose variable into a run against
// whatever happens to listen on the fallback address, so a missing value
// aborts the suite instead.
const requiredEnv = (name: string): string => {
  const value = process.env[name];
  if (!value)
    throw new Error(`${name} must be set; run the suite via scripts/run-bdd-tests.sh`);
  return value;
};

export const getServerAddress = (): [string, number] => {
  const address = requiredEnv('IGGY_TCP_ADDRESS');
  const [host, port] = address.split(':');
  if (!host || !port)
    throw new Error(`IGGY_TCP_ADDRESS must be "host:port", got "${address}"`);
  return [host, parseInt(port, 10)];
};

export const getRootCredentials = () => ({
  username: requiredEnv('IGGY_ROOT_USERNAME'),
  password: requiredEnv('IGGY_ROOT_PASSWORD')
});
