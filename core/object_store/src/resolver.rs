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

//! Host name lookups for the HTTP clients of a shard.

use std::cell::RefCell;
use std::collections::HashMap;
use std::io;
use std::net::{IpAddr, ToSocketAddrs};
use std::time::{Duration, Instant};

use futures::channel::oneshot;
use futures::stream::{self, Stream};
use http::Uri;

/// How long a lookup answer is reused before the next lookup.
const CACHE_TTL: Duration = Duration::from_secs(30);

/// The name of the threads that run the lookups.
const LOOKUP_THREAD: &str = "iggy-dns-lookup";

/// Resolves host names for a `cyper` client on a shard.
///
/// A shard never blocks on a lookup: each one runs on a thread of its own,
/// and the shard awaits the answer. An answer is reused for 30 seconds, so
/// most requests start without a lookup. An address literal needs no lookup.
///
/// Give each client its own resolver with
/// `cyper::ClientBuilder::custom_resolver`.
#[derive(Debug, Default)]
pub struct ShardResolver {
    cache: RefCell<HashMap<String, (Instant, Vec<IpAddr>)>>,
}

impl cyper::resolve::Resolve for ShardResolver {
    type Err = io::Error;

    async fn resolve(&self, uri: &Uri) -> Result<impl Stream<Item = IpAddr> + '_, Self::Err> {
        let host = uri
            .host()
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "the URI has no host"))?;
        let host = host.trim_start_matches('[').trim_end_matches(']');
        if let Ok(address) = host.parse::<IpAddr>() {
            return Ok(stream::iter(vec![address]));
        }
        if let Some((resolved_at, addresses)) = self.cache.borrow().get(host)
            && resolved_at.elapsed() < CACHE_TTL
        {
            return Ok(stream::iter(addresses.clone()));
        }
        let addresses = lookup(host.to_owned()).await?;
        self.cache
            .borrow_mut()
            .insert(host.to_owned(), (Instant::now(), addresses.clone()));
        Ok(stream::iter(addresses))
    }
}

/// The addresses of `host`, from the system resolver on a thread of its own.
async fn lookup(host: String) -> io::Result<Vec<IpAddr>> {
    let (sender, receiver) = oneshot::channel();
    std::thread::Builder::new()
        .name(LOOKUP_THREAD.to_owned())
        .spawn(move || {
            let addresses = (host.as_str(), 0).to_socket_addrs().map(|addresses| {
                let mut found: Vec<IpAddr> = Vec::new();
                for address in addresses {
                    if !found.contains(&address.ip()) {
                        found.push(address.ip());
                    }
                }
                found
            });
            let addresses = addresses.and_then(|found| {
                if found.is_empty() {
                    Err(io::Error::new(
                        io::ErrorKind::NotFound,
                        format!("{host} resolves to no address"),
                    ))
                } else {
                    Ok(found)
                }
            });
            let _ = sender.send(addresses);
        })?;
    receiver
        .await
        .map_err(|_| io::Error::other("the lookup thread stopped without an answer"))?
}

#[cfg(test)]
mod tests {
    use cyper::resolve::Resolve;
    use futures::StreamExt;

    use super::*;

    fn resolved(resolver: &ShardResolver, uri: &str) -> io::Result<Vec<IpAddr>> {
        let uri: Uri = uri.parse().unwrap();
        crate::contract::shard_runtime()
            .block_on(async { Ok(resolver.resolve(&uri).await?.collect::<Vec<_>>().await) })
    }

    #[test]
    fn given_address_literal_when_resolving_should_return_it_without_a_lookup() {
        let resolver = ShardResolver::default();

        let v4 = resolved(&resolver, "http://127.0.0.1:4566/bucket").unwrap();
        let v6 = resolved(&resolver, "http://[::1]:4566/bucket").unwrap();

        assert_eq!(v4, ["127.0.0.1".parse::<IpAddr>().unwrap()]);
        assert_eq!(v6, ["::1".parse::<IpAddr>().unwrap()]);
        assert!(resolver.cache.borrow().is_empty());
    }

    /// The shard runtime of the tests has no blocking pool, so a lookup that
    /// used one would fail here.
    #[test]
    fn given_host_name_when_resolving_on_a_shard_runtime_should_answer_and_cache_it() {
        let resolver = ShardResolver::default();

        let addresses = resolved(&resolver, "http://localhost:4566/").unwrap();

        assert!(addresses.iter().all(IpAddr::is_loopback), "{addresses:?}");
        assert!(resolver.cache.borrow().contains_key("localhost"));
        assert_eq!(resolved(&resolver, "http://localhost/").unwrap(), addresses);
    }

    #[test]
    fn given_unknown_host_when_resolving_should_fail_and_cache_nothing() {
        let resolver = ShardResolver::default();

        let result = resolved(&resolver, "http://iggy-no-such-host.invalid/");

        assert!(result.is_err());
        assert!(resolver.cache.borrow().is_empty());
    }
}
