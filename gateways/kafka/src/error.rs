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

use iggy_kafka_wire::KafkaWireError;
use thiserror::Error;

use crate::bridge::BridgeError;

#[derive(Debug, Error)]
pub enum KafkaProtocolError {
    #[error("invalid server configuration: {0}")]
    InvalidConfig(String),
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("Iggy bridge error: {0}")]
    Bridge(#[from] BridgeError),
    #[error(transparent)]
    Wire(#[from] KafkaWireError),
}

pub type Result<T> = std::result::Result<T, KafkaProtocolError>;
