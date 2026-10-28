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

use crate::offload::offload;
use futures::Stream;
use futures::stream;
use http::Uri;
use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, ToSocketAddrs};
use std::time::{Duration, Instant};

/// How long a resolved address list is reused before the system resolver is asked again.
const CACHE_TTL: Duration = Duration::from_secs(30);

/// Resolves host names through the system resolver on a thread of its own. cyper's default
/// resolver goes through compio's blocking pool, which shard executors do not have, so the
/// first lookup would panic the shard.
#[derive(Default)]
pub(crate) struct OffloadResolver {
    cache: RefCell<HashMap<String, (Instant, Vec<IpAddr>)>>,
}

impl cyper::resolve::Resolve for OffloadResolver {
    type Err = io::Error;

    async fn resolve(&self, uri: &Uri) -> Result<impl Stream<Item = IpAddr> + '_, Self::Err> {
        let host = uri
            .host()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "URI has no host"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(address) = host.parse::<IpAddr>() {
            return Ok(stream::iter(vec![address]));
        }
        if let Some((resolved_at, addresses)) = self.cache.borrow().get(host)
            && resolved_at.elapsed() < CACHE_TTL
        {
            return Ok(stream::iter(addresses.clone()));
        }
        let name = host.to_owned();
        let addresses = offload("iggy-object-store-dns", move || {
            (name.as_str(), 0)
                .to_socket_addrs()
                .map(|addresses| addresses.map(|address| address.ip()).collect::<Vec<_>>())
        })
        .await??;
        if addresses.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::NotFound,
                format!("{host} resolved to no address"),
            ));
        }
        self.cache
            .borrow_mut()
            .insert(host.to_owned(), (Instant::now(), addresses.clone()));
        Ok(stream::iter(addresses))
    }
}
