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

use std::ptr;

use ext_php_rs::{
    convert::IntoZval,
    error::Error,
    exception::PhpException,
    ffi::zend_call_known_function,
    php_class, php_impl,
    types::Zval,
    zend::{ExecutorGlobals, ce},
};
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

#[php_class]
#[php(name = "Iggy\\Exception\\PendingOperationException")]
#[php(extends(IggyException))]
#[derive(Default)]
pub struct PendingOperationException {
    operation_id: String,
    kind: String,
}

#[php_impl]
impl PendingOperationException {
    /// Exact unsigned 64-bit operation ID, represented as a decimal string.
    #[php(getter, change_case = "snake_case")]
    pub fn operation_id(&self) -> &str {
        &self.operation_id
    }

    #[php(getter)]
    pub fn kind(&self) -> &str {
        &self.kind
    }
}

impl PendingOperationException {
    fn into_exception_object(self, message: &str, code: i32) -> Result<Zval, Error> {
        let mut object = self.into_zval(false)?;
        let mut arguments = [message.into_zval(false)?, code.into_zval(false)?];
        let mut result = Zval::new();
        let receiver = object.object_mut().ok_or(Error::Object)?;

        // SAFETY: The initialized object extends Exception, and the argument and result
        // zvals remain live for the call. Its native constructor initializes inherited
        // fields that the generated Rust constructor leaves untouched.
        unsafe {
            zend_call_known_function(
                ce::exception().constructor,
                receiver,
                receiver.ce,
                &raw mut result,
                arguments.len().try_into()?,
                arguments.as_mut_ptr(),
                ptr::null_mut(),
            );
        }
        if let Some(class) = ExecutorGlobals::pending_exception_class() {
            return Err(Error::ExceptionPending { class });
        }
        Ok(object)
    }
}

pub(crate) trait IntoPhpException {
    fn into_php_exception(self) -> PhpException;
}

pub(crate) fn to_php_exception(error: impl IntoPhpException) -> PhpException {
    error.into_php_exception()
}

impl IntoPhpException for IggyError {
    fn into_php_exception(self) -> PhpException {
        let message = self.to_string();

        let pending = match &self {
            IggyError::OperationUncertain(operation_id) => Some((*operation_id, "uncertain")),
            IggyError::LaneBlocked(operation_id) => Some((*operation_id, "lane_blocked")),
            IggyError::OperationNotRetained(operation_id) => Some((*operation_id, "not_retained")),
            _ => None,
        };
        if let Some((operation_id, kind)) = pending {
            let exception = PendingOperationException {
                operation_id: operation_id.to_string(),
                kind: kind.to_owned(),
            };
            return match exception.into_exception_object(&message, self.as_code() as i32) {
                Ok(object) => PhpException::from_message(String::new()).with_object(object),
                Err(error) => error.into_php_exception(),
            };
        }

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
        // A callback exception stays pending in the engine (ExceptionPending), and
        // throwing while one is pending is a no-op, so PHP receives the original.
        PhpException::from_class::<IggyException>(self.to_string())
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
