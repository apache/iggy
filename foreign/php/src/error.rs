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

use ext_php_rs::{convert::IntoZval, error::Error, exception::PhpException, php_class, zend::ce};
use iggy::prelude::IggyError;

#[php_class]
#[php(name = "Iggy\\Exception\\IggyException")]
#[php(extends(ce = ce::exception, stub = "\\Exception"))]
#[derive(Default)]
pub struct IggyException;

#[php_class]
#[php(name = "Iggy\\Exception\\ConnectionException")]
#[php(extends(IggyException))]
#[derive(Default)]
pub struct ConnectionException;

#[php_class]
#[php(name = "Iggy\\Exception\\AuthenticationException")]
#[php(extends(IggyException))]
#[derive(Default)]
pub struct AuthenticationException;

#[php_class]
#[php(name = "Iggy\\Exception\\NotFoundException")]
#[php(extends(IggyException))]
#[derive(Default)]
pub struct NotFoundException;

#[php_class]
#[php(name = "Iggy\\Exception\\TransientException")]
#[php(extends(IggyException))]
#[derive(Default)]
pub struct TransientException;

pub(crate) trait IntoPhpException {
    fn into_php_exception(self) -> PhpException;
}

pub(crate) fn to_php_exception(error: impl IntoPhpException) -> PhpException {
    error.into_php_exception()
}

impl IntoPhpException for IggyError {
    fn into_php_exception(self) -> PhpException {
        let message = self.to_string();

        match PhpExceptionKind::from(&self) {
            PhpExceptionKind::Connection => {
                PhpException::from_class::<ConnectionException>(message)
            }
            PhpExceptionKind::Authentication => {
                PhpException::from_class::<AuthenticationException>(message)
            }
            PhpExceptionKind::NotFound => PhpException::from_class::<NotFoundException>(message),
            PhpExceptionKind::Transient => PhpException::from_class::<TransientException>(message),
            PhpExceptionKind::Base => PhpException::from_class::<IggyException>(message),
        }
    }
}

impl IntoPhpException for Error {
    fn into_php_exception(self) -> PhpException {
        match self {
            Error::Exception(exception) => match exception.into_zval(false) {
                Ok(object) => PhpException::default(String::new()).with_object(object),
                Err(error) => PhpException::from_class::<IggyException>(error.to_string()),
            },
            error => PhpException::from_class::<IggyException>(error.to_string()),
        }
    }
}

impl IntoPhpException for String {
    fn into_php_exception(self) -> PhpException {
        PhpException::from_class::<IggyException>(self)
    }
}

impl IntoPhpException for &str {
    fn into_php_exception(self) -> PhpException {
        PhpException::from_class::<IggyException>(self.to_string())
    }
}

enum PhpExceptionKind {
    Base,
    Connection,
    Authentication,
    NotFound,
    Transient,
}

impl From<&IggyError> for PhpExceptionKind {
    fn from(error: &IggyError) -> Self {
        match error {
            IggyError::Disconnected
            | IggyError::CannotEstablishConnection
            | IggyError::StaleClient
            | IggyError::TcpError
            | IggyError::QuicError
            | IggyError::InvalidServerAddress
            | IggyError::InvalidClientAddress
            | IggyError::InvalidIpAddress(_, _)
            | IggyError::HttpError(_)
            | IggyError::InvalidApiUrl(_)
            | IggyError::NotConnected
            | IggyError::ClientShutdown
            | IggyError::InvalidTlsDomain
            | IggyError::InvalidTlsCertificatePath
            | IggyError::InvalidTlsCertificate
            | IggyError::FailedToAddCertificate
            | IggyError::ConnectionClosed
            | IggyError::CannotCloseWebSocketConnection(_)
            | IggyError::CannotCreateEndpoint
            | IggyError::CannotParseUrl
            | IggyError::WebSocketError
            | IggyError::WebSocketConnectionError
            | IggyError::WebSocketCloseError
            | IggyError::WebSocketReceiveError
            | IggyError::WebSocketSendError
            | IggyError::InvalidConnectionString
            | IggyError::CannotBindToSocket(_) => Self::Connection,
            IggyError::Unauthenticated
            | IggyError::Unauthorized
            | IggyError::InvalidCredentials
            | IggyError::InvalidUsername
            | IggyError::InvalidPassword
            | IggyError::InvalidUserStatus
            | IggyError::UserInactive
            | IggyError::InvalidPersonalAccessToken
            | IggyError::PersonalAccessTokenExpired(_, _)
            | IggyError::JwtMissing
            | IggyError::AccessTokenMissing
            | IggyError::InvalidAccessToken => Self::Authentication,
            IggyError::StateFileNotFound
            | IggyError::ResourceNotFound(_)
            | IggyError::ClientNotFound(_)
            | IggyError::StreamIdNotFound(_)
            | IggyError::StreamNameNotFound(_)
            | IggyError::StreamDirectoryNotFound(_)
            | IggyError::TopicIdNotFound(_, _)
            | IggyError::TopicNameNotFound(_, _)
            | IggyError::TopicDirectoryNotFound(_)
            | IggyError::PartitionNotFound(_, _, _)
            | IggyError::ConsumerOffsetNotFound(_)
            | IggyError::NotResolvedConsumer(_)
            | IggyError::SegmentNotFound
            | IggyError::ConsumerGroupIdNotFound(_, _)
            | IggyError::ConsumerGroupNameNotFound(_, _)
            | IggyError::ConsumerGroupMemberNotFound(_, _, _)
            | IggyError::ShardNotFound(_, _, _) => Self::NotFound,
            IggyError::HttpResponseError(408 | 429 | 500..=599, _)
            | IggyError::EmptyResponse
            | IggyError::CannotSendMessagesDueToClientDisconnection
            | IggyError::BackgroundSendError
            | IggyError::BackgroundSendTimeout
            | IggyError::BackgroundSendBufferFull
            | IggyError::BackgroundWorkerDisconnected
            | IggyError::BackgroundSendBufferOverflow
            | IggyError::ProducerSendFailed { .. }
            | IggyError::TaskTimeout
            | IggyError::ShardCommunicationError => Self::Transient,
            _ => Self::Base,
        }
    }
}
