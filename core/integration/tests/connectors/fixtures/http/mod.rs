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

mod container;
mod sink;
mod source;

pub use sink::{
    HttpSinkIndividualFixture, HttpSinkJsonArrayFixture, HttpSinkMultiTopicFixture,
    HttpSinkNdjsonFixture, HttpSinkNoMetadataFixture, HttpSinkRawFixture,
};
pub use source::{
    GITHUB_ENDPOINT_ID, GITHUB_HMAC_HEADER, GITHUB_INSTANCE, HttpSourceFixture, MANAGEMENT_TOKEN,
    PARTNER_BEARER_TOKEN, PARTNER_ENDPOINT_ID, PARTNER_INSTANCE,
};
