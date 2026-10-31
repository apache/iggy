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
    class::RegisteredClass,
    convert::IntoZval,
    error::Error,
    exception::PhpException,
    ffi::zend_call_known_function,
    php_class, php_impl,
    types::{ZendObject, Zval},
    zend::{ClassEntry, ExecutorGlobals, ce},
};
use iggy::prelude::{IggyError, ReadError};

use crate::receive_message::PollingStrategy;

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
    partition_id: Option<u32>,
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

    #[php(getter, change_case = "snake_case")]
    pub fn partition_id(&self) -> Option<u32> {
        self.partition_id
    }
}

impl PendingOperationException {
    fn from_error(error: &IggyError) -> Option<Self> {
        let (operation_id, kind, partition_id) = match error {
            IggyError::OperationUncertain(operation_id) => (*operation_id, "uncertain", None),
            IggyError::FetchProgressUncertain(operation_id) => {
                (*operation_id, "fetch_progress_uncertain", None)
            }
            IggyError::LaneBlocked(operation_id) => (*operation_id, "lane_blocked", None),
            IggyError::PartitionLaneBlocked(partition_id, operation_id) => {
                (*operation_id, "lane_blocked", Some(*partition_id))
            }
            IggyError::OperationNotRetained(operation_id) => (*operation_id, "not_retained", None),
            _ => return None,
        };
        Some(Self {
            operation_id: operation_id.to_string(),
            kind: kind.to_owned(),
            partition_id,
        })
    }
}

/// A failed read of a PartitionReader or a Consumer. The reader stays usable and retries by
/// itself.
///
/// The code is the code of the error, and the previous exception is the error itself, typed as
/// any other call would throw it, for example a PendingOperationException.
#[php_class]
#[php(name = "Iggy\\Exception\\ReadException")]
#[php(extends(IggyException))]
#[derive(Default)]
pub struct ReadException {
    partition_id: Option<u32>,
    skip: Option<PollingStrategy>,
}

#[php_impl]
impl ReadException {
    /// The partition of the error, or null for an error of the whole reader.
    #[php(getter, change_case = "snake_case")]
    pub fn partition_id(&self) -> Option<u32> {
        self.partition_id
    }

    /// For a message that does not decode: the continuation after it. The reader reads the
    /// message again until a seek of its partition to this continuation skips it.
    #[php(getter)]
    pub fn skip(&self) -> Option<PollingStrategy> {
        self.skip.clone()
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
        let code = self.as_code() as i32;

        match PendingOperationException::from_error(&self) {
            Some(exception) => thrown(exception_object(exception, &message, code, None)),
            None => PhpException::new(message, code, exception_class(&self)),
        }
    }
}

impl IntoPhpException for ReadError {
    fn into_php_exception(self) -> PhpException {
        let message = self.to_string();
        let code = self.error.as_code() as i32;
        let exception = ReadException {
            partition_id: self.partition_id,
            skip: self.skip.map(PollingStrategy::from),
        };

        thrown(
            error_object(&self.error)
                .and_then(|error| exception_object(exception, &message, code, Some(error))),
        )
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

fn thrown(object: Result<Zval, Error>) -> PhpException {
    match object {
        Ok(object) => PhpException::from_message(String::new()).with_object(object),
        Err(error) => error.into_php_exception(),
    }
}

/// An exception object that carries Rust fields or a previous exception, which the generated
/// constructor of its class cannot set.
fn exception_object(
    exception: impl IntoZval,
    message: &str,
    code: i32,
    previous: Option<Zval>,
) -> Result<Zval, Error> {
    let mut object = exception.into_zval(false)?;
    let mut arguments = vec![message.into_zval(false)?, code.into_zval(false)?];
    arguments.extend(previous);
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

/// The exception object that a call failing with `error` throws.
fn error_object(error: &IggyError) -> Result<Zval, Error> {
    let message = error.to_string();
    let code = error.as_code() as i32;

    match PendingOperationException::from_error(error) {
        Some(exception) => exception_object(exception, &message, code, None),
        None => exception_object(
            ZendObject::new(exception_class(error)),
            &message,
            code,
            None,
        ),
    }
}

fn exception_class(error: &IggyError) -> &'static ClassEntry {
    match PhpExceptionKind::from(error) {
        PhpExceptionKind::Connection => ConnectionException::get_metadata().ce(),
        PhpExceptionKind::Authentication => AuthenticationException::get_metadata().ce(),
        PhpExceptionKind::NotFound => NotFoundException::get_metadata().ce(),
        PhpExceptionKind::Transient => TransientException::get_metadata().ce(),
        PhpExceptionKind::Base => IggyException::get_metadata().ce(),
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
            | IggyError::CannotModifyRootUser
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pending_operation_errors_preserve_exact_identity_and_kind() {
        for operation_id in [(1u64 << 53) + 17, u64::MAX] {
            for (error, kind, partition_id) in [
                (
                    IggyError::OperationUncertain(operation_id),
                    "uncertain",
                    None,
                ),
                (IggyError::LaneBlocked(operation_id), "lane_blocked", None),
                (
                    IggyError::FetchProgressUncertain(operation_id),
                    "fetch_progress_uncertain",
                    None,
                ),
                (
                    IggyError::PartitionLaneBlocked(u32::MAX, operation_id),
                    "lane_blocked",
                    Some(u32::MAX),
                ),
                (
                    IggyError::OperationNotRetained(operation_id),
                    "not_retained",
                    None,
                ),
            ] {
                let exception = PendingOperationException::from_error(&error).unwrap();
                assert_eq!(exception.operation_id(), operation_id.to_string());
                assert_eq!(exception.kind(), kind);
                assert_eq!(exception.partition_id(), partition_id);
            }
        }
        assert!(PendingOperationException::from_error(&IggyError::InvalidCommand).is_none());
    }
}
