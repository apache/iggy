/*
 * Licensed to the Apache Software Foundation (ASF) under one
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

// `dedup` is gated `pub(crate)` until the request dispatcher loop wires
// up the lookup / mark / complete / evict_client API end-to-end. Marking
// it crate-private now blocks downstream crates from coupling to a shape
// the dispatcher PR will likely refine (see plan F6).
pub(crate) mod dedup;
pub mod login_register;
pub mod session_manager;
