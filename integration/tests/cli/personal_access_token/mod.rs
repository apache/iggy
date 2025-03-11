/* Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

mod test_pat_create_command;
mod test_pat_delete_command;
mod test_pat_help_command;
mod test_pat_list_command;
// Disable tests due to missing keyring on macOS until #794 is implemented and skip for musl targets
// due to missing keyring support while running tests under cross
#[cfg(not(any(target_os = "macos", target_env = "musl")))]
mod test_pat_login_options;
