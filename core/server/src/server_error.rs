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

use compio_quic::{ConnectionError as QuicConnectionError, ReadError, WriteError};
use error_set::error_set;
use std::array::TryFromSliceError;
use std::io;

error_set!(
    ServerError := ConfigError || ArchiverError || ConnectionError || LogError || CompatError || QuicError

    IoError := {
        #[display("IO error")]
        IoError(io::Error),

        #[display("Write error")]
        WriteError(WriteError),

        #[display("Read error")]
        ReadToEndError(ReadError)
    }

    ConfigError := {
        #[display("Invalid configuration provider: {}", provider_type)]
        InvalidConfigurationProvider { provider_type: String },

        #[display("Cannot load configuration")]
        CannotLoadConfiguration,

        #[display("Invalid configuration")]
        InvalidConfiguration,

        #[display("Cache config validation failure")]
        CacheConfigValidationFailure,
    }

    ArchiverError := {
        #[display("File to archive not found: {}", file_path)]
        FileToArchiveNotFound { file_path: String },

        #[display("Cannot initialize S3 archiver")]
        CannotInitializeS3Archiver,

        #[display("Invalid S3 credentials")]
        InvalidS3Credentials,

        #[display("HTTP request error: {0}")]
        CyperError(cyper::Error),

        #[display("Cannot archive file: {}", file_path)]
        CannotArchiveFile { file_path: String },
    } || IoError

    ConnectionError := {
        #[display("Connection error")]
        QuicConnectionError(QuicConnectionError),
    } || IoError || CommonError

    LogError := {
        #[display("Logging filter reload failure")]
        FilterReloadFailure,

        #[display("Logging stdout reload failure")]
        StdoutReloadFailure,

        #[display("Logging file reload failure")]
        FileReloadFailure,
    }

    CompatError := {
        #[display("Index migration error")]
        IndexMigrationError,
    } || IoError || CommonError

    CommonError := {
        #[display("Try from slice error")]
        TryFromSliceError(TryFromSliceError),

        #[display("SDK error")]
        SdkError(iggy_common::IggyError),
    }

    QuicError := {
        #[display("Cert load error")]
        CertLoadError,
        #[display("Cert generation error")]
        CertGenerationError,
        #[display("Config creation error")]
        ConfigCreationError,
        #[display("Transport config error")]
        TransportConfigError,
    }
);
