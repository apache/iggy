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

use futures_util::StreamExt;
use lapin::{
    Connection, Consumer,
    message::Delivery,
    options::{BasicConsumeOptions, BasicQosOptions},
    types::FieldTable,
};

pub struct RabbitMqDriver {
    connection: Connection,
    consumer: Consumer,
}

#[derive(Debug)]
pub struct DriverError {
    pub operation: &'static str,
    pub error: lapin::Error,
}

impl RabbitMqDriver {
    pub async fn consume(
        connection: Connection,
        queue: &str,
        prefetch_count: u16,
    ) -> Result<Self, DriverError> {
        let setup = async {
            let channel = connection
                .create_channel()
                .await
                .map_err(|error| DriverError {
                    operation: "create_channel",
                    error,
                })?;
            channel
                .basic_qos(prefetch_count, BasicQosOptions::default())
                .await
                .map_err(|error| DriverError {
                    operation: "basic_qos",
                    error,
                })?;
            let consumer = channel
                .basic_consume(
                    queue.into(),
                    "".into(),
                    BasicConsumeOptions {
                        no_ack: false,
                        ..Default::default()
                    },
                    FieldTable::default(),
                )
                .await
                .map_err(|error| DriverError {
                    operation: "basic_consume",
                    error,
                })?;
            Ok::<_, DriverError>(consumer)
        }
        .await;
        let consumer = match setup {
            Ok(value) => value,
            Err(error) => {
                if connection.status().connected() {
                    let _ = connection.close(200, "OK".into()).await;
                }
                return Err(error);
            }
        };
        Ok(Self {
            connection,
            consumer,
        })
    }

    pub async fn next(&mut self) -> Option<lapin::Result<Delivery>> {
        self.consumer.next().await
    }

    pub async fn close(self) -> lapin::Result<()> {
        if self.connection.status().connected() {
            self.connection.close(200, "OK".into()).await
        } else {
            Ok(())
        }
    }
}
