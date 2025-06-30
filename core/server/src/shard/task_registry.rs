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

use async_channel::{Receiver, Sender, bounded};
use futures::future::join_all;
use monoio::task::JoinHandle;
use std::cell::RefCell;
use std::collections::HashMap;
use std::future::Future;
use std::time::Duration;
use tracing::{error, info, warn};

pub struct TaskRegistry {
    tasks: RefCell<Vec<JoinHandle<()>>>,
    active_connections: RefCell<HashMap<u32, Sender<()>>>,
}

impl TaskRegistry {
    pub fn new() -> Self {
        Self {
            tasks: RefCell::new(Vec::new()),
            active_connections: RefCell::new(HashMap::new()),
        }
    }

    pub fn spawn_tracked<F>(&self, future: F)
    where
        F: Future<Output = ()> + 'static,
    {
        let handle = monoio::spawn(future);
        self.tasks.borrow_mut().push(handle);
    }

    pub fn add_connection(&self, client_id: u32) -> Receiver<()> {
        let (stop_sender, stop_receiver) = bounded(1);
        self.active_connections
            .borrow_mut()
            .insert(client_id, stop_sender);
        stop_receiver
    }

    pub fn remove_connection(&self, client_id: &u32) {
        self.active_connections.borrow_mut().remove(client_id);
    }

    pub async fn shutdown_all(&self, timeout: Duration) -> bool {
        info!("Initiating task registry shutdown");

        let connections = self.active_connections.borrow();
        for (client_id, stop_sender) in connections.iter() {
            info!("Sending shutdown signal to client {}", client_id);
            if let Err(e) = stop_sender.send(()).await {
                warn!(
                    "Failed to send shutdown signal to client {}: {}",
                    client_id, e
                );
            }
        }
        drop(connections);

        let tasks = self.tasks.take();
        let total = tasks.len();

        if total == 0 {
            info!("No tasks to shut down");
            return true;
        }

        let timeout_futures: Vec<_> = tasks
            .into_iter()
            .enumerate()
            .map(|(idx, handle)| async move {
                match monoio::time::timeout(timeout, handle).await {
                    Ok(()) => (idx, true),
                    Err(_) => {
                        warn!("Task {} did not complete within timeout", idx);
                        (idx, false)
                    }
                }
            })
            .collect();

        let results = join_all(timeout_futures).await;
        let completed = results.iter().filter(|(_, success)| *success).count();

        info!(
            "Task registry shutdown complete. {} of {} tasks completed",
            completed, total
        );

        completed == total
    }
}
