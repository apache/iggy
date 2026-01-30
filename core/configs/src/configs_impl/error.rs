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

//! Configuration error types.

use std::fmt::Display;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigurationError {
    CannotLoadConfiguration,
    DefaultSerializationFailed,
    DefaultParsingFailed,
    EnvironmentVariableParsingFailed,
    InvalidConfigurationValue,
}

impl Display for ConfigurationError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::CannotLoadConfiguration => write!(f, "Cannot load configuration"),
            Self::DefaultSerializationFailed => {
                write!(f, "Failed to serialize default configuration")
            }
            Self::DefaultParsingFailed => write!(f, "Failed to parse default configuration"),
            Self::EnvironmentVariableParsingFailed => {
                write!(f, "Failed to parse environment variables")
            }
            Self::InvalidConfigurationValue => {
                write!(f, "Provided configuration value is invalid")
            }
        }
    }
}

impl std::error::Error for ConfigurationError {}
