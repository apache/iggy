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

use axum::Json;
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use iggy_common::IggyError;
use serde::Serialize;
use thiserror::Error;
use tracing::error;

#[derive(Debug, Error)]
pub enum CustomError {
    #[error(transparent)]
    Error(#[from] IggyError),
    #[error("Resource not found")]
    ResourceNotFound,
}

#[derive(Debug, Serialize)]
pub struct ErrorResponse {
    pub id: u32,
    pub code: String,
    pub reason: String,
    pub field: Option<String>,
}

impl IntoResponse for CustomError {
    fn into_response(self) -> Response {
        match self {
            CustomError::Error(error) => {
                error!("There was an error: {error}");
                let status_code = match error {
                    IggyError::StreamIdNotFound(_) => StatusCode::NOT_FOUND,
                    IggyError::TopicIdNotFound(_, _) => StatusCode::NOT_FOUND,
                    IggyError::PartitionNotFound(_, _, _) => StatusCode::NOT_FOUND,
                    IggyError::SegmentNotFound => StatusCode::NOT_FOUND,
                    IggyError::ClientNotFound(_) => StatusCode::NOT_FOUND,
                    IggyError::ConsumerGroupIdNotFound(_, _) => StatusCode::NOT_FOUND,
                    IggyError::ConsumerGroupNameNotFound(_, _) => StatusCode::NOT_FOUND,
                    IggyError::ConsumerGroupMemberNotFound(_, _, _) => StatusCode::NOT_FOUND,
                    IggyError::ConsumerOffsetNotFound(_) => StatusCode::NOT_FOUND,
                    IggyError::ResourceNotFound(_) => StatusCode::NOT_FOUND,
                    IggyError::Unauthenticated => StatusCode::UNAUTHORIZED,
                    IggyError::AccessTokenMissing => StatusCode::UNAUTHORIZED,
                    IggyError::InvalidAccessToken => StatusCode::UNAUTHORIZED,
                    IggyError::InvalidPersonalAccessToken => StatusCode::UNAUTHORIZED,
                    IggyError::Unauthorized => StatusCode::FORBIDDEN,
                    _ => StatusCode::BAD_REQUEST,
                };
                (status_code, Json(ErrorResponse::from_error(error)))
            }
            CustomError::ResourceNotFound => (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    id: 404,
                    code: "not_found".to_string(),
                    reason: "Resource not found".to_string(),
                    field: None,
                }),
            ),
        }
        .into_response()
    }
}

impl ErrorResponse {
    pub fn from_error(error: IggyError) -> Self {
        ErrorResponse {
            id: error.as_code(),
            code: error.as_string().to_string(),
            reason: error.to_string(),
            field: match error {
                IggyError::StreamIdNotFound(_) => Some("stream_id".to_string()),
                IggyError::TopicIdNotFound(_, _) => Some("topic_id".to_string()),
                IggyError::PartitionNotFound(_, _, _) => Some("partition_id".to_string()),
                IggyError::SegmentNotFound => Some("segment_id".to_string()),
                IggyError::ClientNotFound(_) => Some("client_id".to_string()),
                IggyError::InvalidStreamName => Some("name".to_string()),
                IggyError::StreamNameAlreadyExists(_) => Some("name".to_string()),
                IggyError::InvalidTopicName => Some("name".to_string()),
                IggyError::TopicNameAlreadyExists(_, _) => Some("name".to_string()),
                IggyError::InvalidStreamId => Some("stream_id".to_string()),
                IggyError::InvalidTopicId => Some("topic_id".to_string()),
                IggyError::InvalidOffset(_) => Some("offset".to_string()),
                IggyError::InvalidConsumerGroupId => Some("consumer_group_id".to_string()),
                IggyError::ConsumerGroupNameAlreadyExists(_, _) => Some("name".to_string()),
                IggyError::UserAlreadyExists => Some("username".to_string()),
                IggyError::PersonalAccessTokenAlreadyExists(_, _) => Some("name".to_string()),
                _ => None,
            },
        }
    }
}
