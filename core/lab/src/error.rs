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

use iggy_common::IggyError;

#[derive(Debug, thiserror::Error)]
pub enum LabError {
    #[error(transparent)]
    Iggy(#[from] IggyError),

    #[error("operation timed out: {context}")]
    Timeout { context: String },

    #[error("{context}: {source}")]
    Io {
        context: String,
        source: std::io::Error,
    },

    #[error("trace parse error at {path}:{line}: {message}")]
    TraceParse {
        path: String,
        line: usize,
        message: String,
    },

    #[error("invalid identifier: {0}")]
    InvalidIdentifier(String),

    #[error("prefix safety violation: {0}")]
    PrefixViolation(String),
}
