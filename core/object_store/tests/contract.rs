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

//! The contract every provider meets, run against `fs` and against MinIO. Every case runs on the
//! shard executor, which has no blocking pool, so a provider that needs one panics here.

use bytes::Bytes;
use iggy_object_store::fs::FsStore;
use iggy_object_store::s3::{Credentials, S3Config, S3Store};
use iggy_object_store::{ObjectStore, ObjectStoreError, PutBody, PutOptions};
use std::future::Future;

fn run(future: impl Future<Output = ()>) {
    // The server installs the provider at boot, before any shard builds a client.
    let _ = rustls::crypto::ring::default_provider().install_default();
    server_common::create_shard_executor()
        .expect("shard executor")
        .block_on(future);
}

fn unique_prefix() -> String {
    format!("contract-{:016x}", rand::random::<u64>())
}

macro_rules! contract {
    ($($case:ident),* $(,)?) => {
        mod fs_store {
            $(
                #[test]
                fn $case() {
                    super::run(async {
                        let directory = tempfile::tempdir().unwrap();
                        let store = super::FsStore::open(directory.path()).await.unwrap();
                        super::cases::$case(&store, &super::unique_prefix()).await;
                    });
                }
            )*
        }

        mod s3_store {
            $(
                #[test]
                fn $case() {
                    let config = super::minio::config();
                    super::run(async {
                        let store = super::S3Store::new(config).unwrap();
                        store.create_bucket().await.unwrap();
                        super::cases::$case(&store, &super::unique_prefix()).await;
                    });
                }
            )*
        }
    };
}

contract!(
    given_object_when_putting_should_head_size_and_metadata,
    given_object_when_reading_ranges_should_return_requested_bytes,
    given_existing_key_when_putting_if_absent_should_refuse_and_keep_original,
    given_missing_key_when_reading_should_return_not_found,
    given_file_body_when_putting_should_store_exact_slice,
    given_short_file_when_putting_should_fail_as_source_error_and_store_nothing,
    given_wrong_crc32c_when_putting_should_fail_and_store_nothing,
    given_many_keys_when_listing_should_page_in_key_order,
    given_keys_when_deleting_should_remove_them_and_ignore_missing,
    given_invalid_key_when_calling_should_reject_before_any_io,
);

mod minio {
    use super::{Credentials, S3Config};
    use std::sync::OnceLock;
    use testcontainers::core::{IntoContainerPort, WaitFor};
    use testcontainers::runners::SyncRunner;
    use testcontainers::{GenericImage, ImageExt, ReuseDirective};

    const IMAGE: &str = "minio/minio";
    const TAG: &str = "RELEASE.2025-09-07T16-13-09Z";
    const PORT: u16 = 9000;
    const USER: &str = "iggy-contract";
    const PASSWORD: &str = "iggy-contract-secret";

    static ENDPOINT: OnceLock<String> = OnceLock::new();

    pub fn config() -> S3Config {
        S3Config {
            endpoint: ENDPOINT.get_or_init(start).clone(),
            region: "us-east-1".to_owned(),
            bucket: "iggy-contract".to_owned(),
            path_style: true,
            credentials: Credentials {
                access_key_id: USER.to_owned(),
                secret_access_key: PASSWORD.to_owned(),
                session_token: None,
            },
        }
    }

    /// Starts one MinIO shared by every test and by later runs. A reused container survives
    /// the handle's drop, and each test works under its own prefix.
    fn start() -> String {
        let container = GenericImage::new(IMAGE, TAG)
            .with_exposed_port(PORT.tcp())
            .with_wait_for(WaitFor::message_on_either_std("API:"))
            .with_env_var("MINIO_ROOT_USER", USER)
            .with_env_var("MINIO_ROOT_PASSWORD", PASSWORD)
            .with_cmd(["server", "/data"])
            .with_container_name("iggy-object-store-minio")
            .with_reuse(ReuseDirective::Always)
            .start()
            .expect("MinIO container (Docker is required)");
        let port = container.get_host_port_ipv4(PORT).expect("MinIO port");
        // A host name rather than an address, so the resolver runs too.
        format!("http://localhost:{port}")
    }
}

mod cases {
    use super::*;
    use compio::fs::File;
    use std::io::Write;

    fn bytes(len: usize) -> Vec<u8> {
        (0..len)
            .map(|index| (index * 31 + index / 251) as u8)
            .collect()
    }

    fn metadata(pairs: &[(&str, &str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(key, value)| ((*key).to_owned(), (*value).to_owned()))
            .collect()
    }

    async fn put_bytes(store: &impl ObjectStore, key: &str, data: &[u8]) {
        store
            .put(
                key,
                PutBody::Bytes(Bytes::copy_from_slice(data)),
                &PutOptions::default(),
            )
            .await
            .unwrap();
    }

    async fn read_all(store: &impl ObjectStore, key: &str) -> Bytes {
        let size = store.head(key).await.unwrap().size;
        store.get_range(key, 0..size).await.unwrap()
    }

    pub async fn given_object_when_putting_should_head_size_and_metadata(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/1/2/3/00000000000000000000.log");
        let options = PutOptions {
            metadata: metadata(&[("start-offset", "0"), ("end-offset", "99")]),
            ..PutOptions::default()
        };
        store
            .put(
                &key,
                PutBody::Bytes(Bytes::from_static(b"segment bytes")),
                &options,
            )
            .await
            .unwrap();

        let head = store.head(&key).await.unwrap();

        assert_eq!(head.size, 13);
        assert_eq!(
            head.metadata,
            metadata(&[("end-offset", "99"), ("start-offset", "0")])
        );
        assert!(!head.etag.is_empty());
    }

    pub async fn given_object_when_reading_ranges_should_return_requested_bytes(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/ranges");
        let data = bytes(256);
        put_bytes(store, &key, &data).await;

        assert_eq!(store.get_range(&key, 0..256).await.unwrap(), data);
        assert_eq!(store.get_range(&key, 10..20).await.unwrap(), data[10..20]);
        assert_eq!(store.get_range(&key, 250..300).await.unwrap(), data[250..]);
        assert!(matches!(
            store.get_range(&key, 256..300).await,
            Err(ObjectStoreError::InvalidRange)
        ));
    }

    pub async fn given_existing_key_when_putting_if_absent_should_refuse_and_keep_original(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/conditional");
        let if_absent = PutOptions {
            if_absent: true,
            ..PutOptions::default()
        };
        let put = |data: &'static [u8], options: &PutOptions| {
            let options = options.clone();
            let key = key.clone();
            async move {
                store
                    .put(&key, PutBody::Bytes(Bytes::from_static(data)), &options)
                    .await
            }
        };

        put(b"first", &if_absent).await.unwrap();
        let refused = put(b"second", &if_absent).await;
        assert!(matches!(refused, Err(ObjectStoreError::PreconditionFailed)));
        assert_eq!(read_all(store, &key).await, &b"first"[..]);

        put(b"third", &PutOptions::default()).await.unwrap();
        assert_eq!(read_all(store, &key).await, &b"third"[..]);
    }

    pub async fn given_missing_key_when_reading_should_return_not_found(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/missing");
        assert!(matches!(
            store.head(&key).await,
            Err(ObjectStoreError::NotFound)
        ));
        assert!(matches!(
            store.get_range(&key, 0..10).await,
            Err(ObjectStoreError::NotFound)
        ));
    }

    /// Several pieces long, so the body streams with an explicit length.
    pub async fn given_file_body_when_putting_should_store_exact_slice(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/streamed.log");
        let data = bytes(3 * 1024 * 1024 + 1234);
        let (offset, len) = (7, data.len() - 17);
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&data).unwrap();
        let file = File::open(source.path()).await.unwrap();
        let options = PutOptions {
            crc32c: Some(crc32c::crc32c(&data[offset..offset + len])),
            ..PutOptions::default()
        };

        let body = PutBody::File {
            file,
            offset: offset as u64,
            len: len as u64,
            pacer: None,
        };
        store.put(&key, body, &options).await.unwrap();

        assert_eq!(store.head(&key).await.unwrap().size, len as u64);
        assert_eq!(read_all(store, &key).await, data[offset..offset + len]);
    }

    pub async fn given_short_file_when_putting_should_fail_as_source_error_and_store_nothing(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/short.log");
        let mut source = tempfile::NamedTempFile::new().unwrap();
        source.write_all(&bytes(100)).unwrap();
        let file = File::open(source.path()).await.unwrap();

        let body = PutBody::File {
            file,
            offset: 0,
            len: 2 * 1024 * 1024,
            pacer: None,
        };
        let result = store.put(&key, body, &PutOptions::default()).await;

        assert!(
            matches!(result, Err(ObjectStoreError::Source(_))),
            "{result:?}"
        );
        assert!(matches!(
            store.head(&key).await,
            Err(ObjectStoreError::NotFound)
        ));
    }

    pub async fn given_wrong_crc32c_when_putting_should_fail_and_store_nothing(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let key = format!("{prefix}/checksummed");
        let data = bytes(4096);
        let options = PutOptions {
            crc32c: Some(crc32c::crc32c(&data) ^ 1),
            ..PutOptions::default()
        };

        let result = store
            .put(&key, PutBody::Bytes(Bytes::from(data)), &options)
            .await;

        assert!(
            matches!(result, Err(ObjectStoreError::ChecksumMismatch)),
            "{result:?}"
        );
        assert!(matches!(
            store.head(&key).await,
            Err(ObjectStoreError::NotFound)
        ));
    }

    pub async fn given_many_keys_when_listing_should_page_in_key_order(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let total = 1003;
        let keys: Vec<String> = (0..total)
            .rev()
            .map(|index| format!("{prefix}/list/{index:05}"))
            .collect();
        for batch in keys.chunks(64) {
            futures::future::join_all(batch.iter().map(|key| put_bytes(store, key, b"x"))).await;
        }
        put_bytes(store, &format!("{prefix}/other/00000"), b"x").await;

        let first = store.list(&format!("{prefix}/list/"), None).await.unwrap();
        let second = store
            .list(&format!("{prefix}/list/"), first.next.as_deref())
            .await
            .unwrap();
        let partial = store
            .list(&format!("{prefix}/list/0000"), None)
            .await
            .unwrap();

        let mut expected = keys.clone();
        expected.sort();
        let listed: Vec<String> = first
            .objects
            .iter()
            .chain(&second.objects)
            .map(|object| object.key.clone())
            .collect();
        assert_eq!(first.objects.len(), 1000);
        assert!(first.next.is_some());
        assert_eq!(second.next, None);
        assert_eq!(listed, expected);
        assert!(first.objects.iter().all(|object| object.size == 1));
        assert_eq!(partial.objects.len(), 10);
        assert_eq!(partial.next, None);
    }

    pub async fn given_keys_when_deleting_should_remove_them_and_ignore_missing(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let keys = vec![
            format!("{prefix}/delete/a"),
            format!("{prefix}/delete/b"),
            format!("{prefix}/delete/missing"),
        ];
        put_bytes(store, &keys[0], b"a").await;
        put_bytes(store, &keys[1], b"b").await;

        store.delete(&keys).await.unwrap();
        store.delete(&keys).await.unwrap();

        assert!(matches!(
            store.head(&keys[0]).await,
            Err(ObjectStoreError::NotFound)
        ));
        let listed = store
            .list(&format!("{prefix}/delete/"), None)
            .await
            .unwrap();
        assert!(listed.objects.is_empty(), "{listed:?}");
    }

    pub async fn given_invalid_key_when_calling_should_reject_before_any_io(
        store: &impl ObjectStore,
        prefix: &str,
    ) {
        let body = || PutBody::Bytes(Bytes::from_static(b"x"));
        let escaping = format!("{prefix}/../escape");
        assert!(matches!(
            store.put(&escaping, body(), &PutOptions::default()).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.head(&format!("{prefix}/.hidden")).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
        assert!(matches!(
            store.list(&format!("{prefix}//"), None).await,
            Err(ObjectStoreError::InvalidKey(_))
        ));
    }
}
