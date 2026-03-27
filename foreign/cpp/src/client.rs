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

use crate::{RUNTIME, ffi};
use iggy::prelude::{
    Client as IggyConnectionClient, CompressionAlgorithm as RustCompressionAlgorithm,
    ConsumerGroupClient, Identifier as RustIdentifier, IggyClient as RustIggyClient,
    IggyClientBuilder as RustIggyClientBuilder, IggyError, IggyExpiry as RustIggyExpiry,
    MaxTopicSize as RustMaxTopicSize, PartitionClient, StreamClient, TopicClient, UserClient,
};
use std::str::FromStr;
use std::sync::Arc;

pub struct Client {
    pub inner: Arc<RustIggyClient>,
}

pub fn new_connection(connection_string: String) -> Result<*mut Client, String> {
    let connection_str = connection_string.as_str();
    let client = match connection_str {
        "" => RustIggyClientBuilder::new()
            .with_tcp()
            .build()
            .map_err(|error| format!("Could not build default connection: {error}"))?,
        s if s.starts_with("iggy://") || s.starts_with("iggy+") => {
            RustIggyClient::from_connection_string(s)
                .map_err(|error| format!("Could not parse connection string '{}': {error}", s))?
        }
        s => RustIggyClientBuilder::new()
            .with_tcp()
            .with_server_address(connection_string.clone())
            .build()
            .map_err(|error| format!("Could not build connection for address '{}': {error}", s))?,
    };

    Ok(Box::into_raw(Box::new(Client {
        inner: Arc::new(client),
    })))
}

impl Client {
    pub fn login_user(&self, username: String, password: String) -> Result<(), String> {
        RUNTIME.block_on(async {
            self.inner
                .login_user(&username, &password)
                .await
                .map_err(|error| format!("Could not login user '{}': {error}", username))?;
            Ok(())
        })
    }

    pub fn connect(&self) -> Result<(), String> {
        RUNTIME.block_on(async {
            self.inner
                .connect()
                .await
                .map_err(|error| format!("Could not connect: {error}"))?;
            Ok(())
        })
    }

    pub fn create_stream(&self, stream_name: String) -> Result<(), String> {
        RUNTIME.block_on(async {
            self.inner
                .create_stream(&stream_name)
                .await
                .map_err(|error| format!("Could not create stream '{}': {error}", stream_name))?;
            Ok(())
        })
    }

    pub fn get_stream(&self, stream_id: ffi::Identifier) -> Result<ffi::StreamDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not get stream: {error}"))?;

        RUNTIME.block_on(async {
            let stream_details = self
                .inner
                .get_stream(&rust_stream_id)
                .await
                .map_err(|error| format!("Could not get stream '{}': {error}", rust_stream_id))?;
            let stream_details = stream_details
                .ok_or_else(|| format!("Stream '{}' was not found", rust_stream_id))?;
            Ok(ffi::StreamDetails::from(stream_details))
        })
    }

    pub fn delete_stream(&self, stream_id: ffi::Identifier) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not delete stream: {error}"))?;

        RUNTIME.block_on(async {
            self.inner
                .delete_stream(&rust_stream_id)
                .await
                .map_err(|error| {
                    format!("Could not delete stream '{}': {error}", rust_stream_id)
                })?;
            Ok(())
        })
    }

    // pub fn purge_stream(&self, stream_id: ffi::Identifier) -> Result<(), String> {
    //     let rust_stream_id = RustIdentifier::try_from(stream_id)
    //         .map_err(|error| format!("Could not purge stream: {error}"))?;

    //     RUNTIME.block_on(async {
    //         self.inner
    //             .purge_stream(&rust_stream_id)
    //             .await
    //             .map_err(|error| format!("Could not purge stream '{}': {error}", rust_stream_id))?;
    //         Ok(())
    //     })
    // }

    #[allow(clippy::too_many_arguments)]
    pub fn create_topic(
        &self,
        stream_id: ffi::Identifier,
        topic_name: String,
        partitions_count: u32,
        compression_algorithm: String,
        replication_factor: u8,
        message_expiry_kind: String,
        message_expiry_value: u64,
        max_topic_size: String,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id)
            .map_err(|error| format!("Could not create topic '{}': {error}", topic_name))?;
        let rust_compression_algorithm = match compression_algorithm.to_lowercase().as_str() {
            "" | "none" => RustCompressionAlgorithm::None,
            _ => RustCompressionAlgorithm::from_str(&compression_algorithm).map_err(|error| {
                format!(
                    "Could not create topic '{}': invalid compression algorithm '{}': {error}",
                    topic_name, compression_algorithm
                )
            })?,
        };
        let rust_replication_factor = match replication_factor {
            0 => None,
            value => Some(value),
        };
        let rust_message_expiry = match message_expiry_kind.as_str() {
            "" | "server_default" | "default" => RustIggyExpiry::ServerDefault,
            "never_expire" => RustIggyExpiry::NeverExpire,
            "duration" => RustIggyExpiry::ExpireDuration(iggy::prelude::IggyDuration::from(
                message_expiry_value,
            )),
            _ => {
                return Err(format!(
                    "Could not create topic '{}': invalid message expiry kind '{}'",
                    topic_name, message_expiry_kind
                ));
            }
        };
        let rust_max_topic_size = match max_topic_size.as_str() {
            "" | "server_default" | "0" => RustMaxTopicSize::ServerDefault,
            _ => RustMaxTopicSize::from_str(&max_topic_size).map_err(|error| {
                format!(
                    "Could not create topic '{}': invalid max topic size '{}': {error}",
                    topic_name, max_topic_size
                )
            })?,
        };

        RUNTIME.block_on(async {
            self.inner
                .create_topic(
                    &rust_stream_id,
                    &topic_name,
                    partitions_count,
                    rust_compression_algorithm,
                    rust_replication_factor,
                    rust_message_expiry,
                    rust_max_topic_size,
                )
                .await
                .map_err(|error| {
                    format!(
                        "Could not create topic '{}' on stream '{}': {error}",
                        topic_name, rust_stream_id
                    )
                })?;
            Ok(())
        })
    }

    // pub fn purge_topic(
    //     &self,
    //     stream_id: ffi::Identifier,
    //     topic_id: ffi::Identifier,
    // ) -> Result<(), String> {
    //     let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
    //         format!("Could not purge topic: invalid stream identifier: {error}")
    //     })?;
    //     let rust_topic_id = RustIdentifier::try_from(topic_id)
    //         .map_err(|error| format!("Could not purge topic: invalid topic identifier: {error}"))?;

    //     RUNTIME.block_on(async {
    //         self.inner
    //             .purge_topic(&rust_stream_id, &rust_topic_id)
    //             .await
    //             .map_err(|error| {
    //                 format!(
    //                     "Could not purge topic '{}' on stream '{}': {error}",
    //                     rust_topic_id, rust_stream_id
    //                 )
    //             })?;
    //         Ok(())
    //     })
    // }

    pub fn create_partitions(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        partitions_count: u32,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not create partitions: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not create partitions: invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .create_partitions(&rust_stream_id, &rust_topic_id, partitions_count)
                .await
                .map_err(|error| {
                    format!(
                        "Could not create {partitions_count} partitions for topic '{}' on stream '{}': {error}",
                        rust_topic_id, rust_stream_id
                    )
                })?;
            Ok(())
        })
    }

    pub fn delete_partitions(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        partitions_count: u32,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not delete partitions: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not delete partitions: invalid topic identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .delete_partitions(&rust_stream_id, &rust_topic_id, partitions_count)
                .await
                .map_err(|error| {
                    format!(
                        "Could not delete {partitions_count} partitions for topic '{}' on stream '{}': {error}",
                        rust_topic_id, rust_stream_id
                    )
                })?;
            Ok(())
        })
    }

    pub fn create_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        name: String,
    ) -> Result<ffi::ConsumerGroupDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!(
                "Could not create consumer group '{}': invalid stream identifier: {error}",
                name
            )
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!(
                "Could not create consumer group '{}': invalid topic identifier: {error}",
                name
            )
        })?;

        RUNTIME.block_on(async {
            let group = self
                .inner
                .create_consumer_group(&rust_stream_id, &rust_topic_id, &name)
                .await
                .map_err(|error| {
                    format!(
                        "Could not create consumer group '{}' for topic '{}' on stream '{}': {error}",
                        name, rust_topic_id, rust_stream_id
                    )
                })?;
            Ok(ffi::ConsumerGroupDetails::from(group))
        })
    }

    pub fn get_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        group_id: ffi::Identifier,
    ) -> Result<ffi::ConsumerGroupDetails, String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not get consumer group: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not get consumer group: invalid topic identifier: {error}")
        })?;
        let rust_group_id = RustIdentifier::try_from(group_id).map_err(|error| {
            format!("Could not get consumer group: invalid group identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            let group = self
                .inner
                .get_consumer_group(&rust_stream_id, &rust_topic_id, &rust_group_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not get consumer group '{}' for topic '{}' on stream '{}': {error}",
                        rust_group_id, rust_topic_id, rust_stream_id
                    )
                })?;
            let group = group.ok_or_else(|| {
                format!(
                    "Consumer group '{}' was not found for topic '{}' on stream '{}'",
                    rust_group_id, rust_topic_id, rust_stream_id
                )
            })?;
            Ok(ffi::ConsumerGroupDetails::from(group))
        })
    }

    pub fn delete_consumer_group(
        &self,
        stream_id: ffi::Identifier,
        topic_id: ffi::Identifier,
        group_id: ffi::Identifier,
    ) -> Result<(), String> {
        let rust_stream_id = RustIdentifier::try_from(stream_id).map_err(|error| {
            format!("Could not delete consumer group: invalid stream identifier: {error}")
        })?;
        let rust_topic_id = RustIdentifier::try_from(topic_id).map_err(|error| {
            format!("Could not delete consumer group: invalid topic identifier: {error}")
        })?;
        let rust_group_id = RustIdentifier::try_from(group_id).map_err(|error| {
            format!("Could not delete consumer group: invalid group identifier: {error}")
        })?;

        RUNTIME.block_on(async {
            self.inner
                .delete_consumer_group(&rust_stream_id, &rust_topic_id, &rust_group_id)
                .await
                .map_err(|error| {
                    format!(
                        "Could not delete consumer group '{}' for topic '{}' on stream '{}': {error}",
                        rust_group_id, rust_topic_id, rust_stream_id
                    )
                })?;
            Ok(())
        })
    }
}

pub unsafe fn delete_connection(client: *mut Client) -> Result<(), String> {
    if client.is_null() {
        return Ok(());
    }

    // TODO(slbotbm): Address comment from @hubcio: if logout_user will fail you will have a leak, this will be tagged by e.g. valgrind if someone will test iggy rigorously
    let logout_result = RUNTIME.block_on(async { unsafe { &*client }.inner.logout_user().await });

    unsafe {
        drop(Box::from_raw(client));
    }

    match logout_result {
        Ok(()) | Err(IggyError::Unauthenticated | IggyError::Disconnected) => Ok(()),
        Err(error) => Err(format!(
            "Could not logout user during deletion of client: {error}"
        )),
    }
}
