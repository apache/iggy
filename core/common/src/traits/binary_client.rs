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

use crate::{BinaryTransport, Client};
use async_trait::async_trait;

/// A client that can send and receive binary messages. In `vsr` builds it
/// also exposes the sealed [`VsrSessionControl`](crate::VsrSessionControl)
/// for the SDK's login/logout flows; that surface stays out of
/// [`BinaryTransport`] so external `&dyn BinaryTransport` consumers can't
/// touch consensus session state.
#[cfg(feature = "vsr")]
#[async_trait]
pub trait BinaryClient: BinaryTransport + Client + crate::VsrSessionControl {}

#[cfg(not(feature = "vsr"))]
#[async_trait]
pub trait BinaryClient: BinaryTransport + Client {}
