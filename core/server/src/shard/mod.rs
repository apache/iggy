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

pub mod builder;
pub mod connector;
pub mod frame;
pub mod namespace;

use ahash::HashMap;
use builder::IggyShardBuilder;
use connector::{Receiver, ShardConnector, StopReceiver, StopSender};
use frame::ShardFrame;
use iggy_common::IggyError;
use namespace::IggyNamespace;
use tracing::info;
use std::{
    cell::{Cell, RefCell},
    rc::Rc,
    sync::Arc, time::Instant,
};

use crate::{
    bootstrap::create_root_user, configs::server::ServerConfig, state::{file::FileState, StateKind},
    streaming::storage::SystemStorage,
};
pub(crate) struct Shard {
    id: u16,
    connection: ShardConnector<ShardFrame>,
}

impl Shard {
    pub fn new(connection: ShardConnector<ShardFrame>) -> Self {
        Self {
            id: connection.id,
            connection,
        }
    }
}

struct ShardInfo {
    id: u16,
}

pub struct IggyShard {
    pub id: u16,
    shards: Vec<Shard>,
    shards_table: RefCell<HashMap<IggyNamespace, ShardInfo>>,

    //pub(crate) permissioner: RefCell<Permissioner>,
    //pub(crate) streams: RwLock<HashMap<u32, Stream>>,
    //pub(crate) streams_ids: RefCell<HashMap<String, u32>>,
    //pub(crate) users: RefCell<HashMap<UserId, User>>,
    // TODO: Refactor.
    pub(crate) storage: Rc<SystemStorage>,

    // TODO - get rid of this dynamic dispatch.
    pub(crate) state: Rc<StateKind>,
    //pub(crate) encryptor: Option<Rc<dyn Encryptor>>,
    config: ServerConfig,
    //pub(crate) client_manager: RefCell<ClientManager>,
    //pub(crate) active_sessions: RefCell<Vec<Session>>,
    //pub(crate) metrics: Metrics,
    pub frame_receiver: Cell<Option<Receiver<ShardFrame>>>,
    stop_receiver: StopReceiver,
    stop_sender: StopSender,
}

impl IggyShard {
    pub fn builder() -> IggyShardBuilder {
        Default::default()
    }

    pub async fn init(&mut self) -> Result<(), IggyError> {
        let now = Instant::now();
        //TODO: Fix this either by moving it to main function, or by using `run_once` barrier.
        //let state_entries = self.state.init().await?;
        //let system_state = SystemState::init(state_entries).await?;
        //let user = create_root_user();
        self.load_state().await;
        self.load_users().await;
        // Add default root user.
        self.load_streams().await;
        //TODO: Fix the archiver.
        /*
        if let Some(archiver) = self.archiver.as_ref() {
            archiver
                .init()
                .await
                .expect("Failed to initialize archiver");
        }
        */
        info!("Initialized system in {} ms.", now.elapsed().as_millis());
        Ok(())
    }

    async fn load_state(&self) {
        todo!()
    }

    async fn load_users(&self) {
        todo!()
    }

    async fn load_streams(&self) {
        todo!()
    }

    pub fn assert_init(&self) {}
}
