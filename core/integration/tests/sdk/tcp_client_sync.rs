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

use std::sync::Arc;

use iggy::prelude::*;
use integration::test_server::{TestServer, IpAddrKind, ClientFactory};
use integration::tcp_client_sync::TcpClientSyncFactory;
use serial_test::serial;
use bytes::Bytes;

#[tokio::test]
#[serial]
async fn should_connect_and_ping_tcp_sync_client() {
    let mut test_server = TestServer::default();
    test_server.start();

    let tcp_client_config = TcpClientConfig {
        server_address: test_server.get_raw_tcp_addr().unwrap(),
        ..TcpClientConfig::default()
    };
    let client = ClientWrapper::TcpSync(Box::new(TcpClientSyncTcp::create_tcp(Arc::new(tcp_client_config)).unwrap()));
    let client = IggyClient::create(client, None, None);

    client.connect().await.unwrap();
    assert!(client.ping().await.is_ok(), "Failed to ping server");
    
    // let factory = TcpClientSyncFactory {
    //     server_addr: test_server.get_raw_tcp_addr().unwrap(),
    //     nodelay: false,
    //     tls_enabled: false,
    //     tls_domain: "localhost".to_string(),
    //     tls_ca_file: None,
    //     tls_validate_certificate: false,
    // };
    
    // let client_wrapper = factory.create_client().await;
    
    // Test direct ping on the wrapper (which should already be connected)
    // match &client_wrapper {
    //     ClientWrapper::TcpSync(sync_client) => {
    //         sync_client.ping().await.unwrap();
            
    //         // Test login as root user
    //         sync_client
    //             .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
    //             .await
    //             .unwrap();
    //     }
    //     _ => panic!("Expected TcpSync client"),
    // }
    
    test_server.stop();
}

#[tokio::test]
#[serial]
async fn should_perform_basic_streaming_operations() {
    let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
    server.start();
    
    let factory = TcpClientSyncFactory {
        server_addr: server.get_raw_tcp_addr().unwrap(),
        nodelay: false,
        tls_enabled: false,
        tls_domain: "localhost".to_string(),
        tls_ca_file: None,
        tls_validate_certificate: false,
    };
    
    let client_wrapper = factory.create_client().await;
    let client = IggyClient::create(client_wrapper, None, None);
    
    // Connect first
    client.connect().await.unwrap();
    
    // Login as root
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    
    let stream_id = 1;
    let topic_id = 1;
    let stream_name = "test-stream";
    let topic_name = "test-topic";
    
    // Create stream
    let stream = client
        .create_stream(stream_name, Some(stream_id))
        .await
        .unwrap();
    assert_eq!(stream.id, stream_id);
    assert_eq!(stream.name, stream_name);
    
    // Create topic
    let topic = client
        .create_topic(
            &Identifier::numeric(stream_id).unwrap(),
            topic_name,
            1, // partitions_count
            Default::default(), // compression_algorithm
            None, // replication_factor
            Some(topic_id),
            IggyExpiry::NeverExpire, // message_expiry
            MaxTopicSize::ServerDefault, // max_topic_size
        )
        .await
        .unwrap();
    assert_eq!(topic.id, topic_id);
    assert_eq!(topic.name, topic_name);
    
    // Send a message
    let payload = Bytes::from("Hello, World!");
    let mut message = IggyMessage::builder()
        .payload(payload.clone())
        .build()
        .unwrap();
    client
        .send_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            &Partitioning::partition_id(1),
            &mut [message],
        )
        .await
        .unwrap();
    
    // Poll the message
    let consumer = Consumer::default();
    let polled = client
        .poll_messages(
            &Identifier::numeric(stream_id).unwrap(),
            &Identifier::numeric(topic_id).unwrap(),
            Some(1), // partition_id
            &consumer,
            &PollingStrategy::next(),
            1, // count
            false, // auto_commit
        )
        .await
        .unwrap();
    
    assert_eq!(polled.messages.len(), 1);
    assert_eq!(
        polled.messages[0].payload,
        payload
    );
    
    server.stop();
}

#[tokio::test] 
#[serial]
async fn should_handle_authentication() {
    let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
    server.start();
    
    let factory = TcpClientSyncFactory {
        server_addr: server.get_raw_tcp_addr().unwrap(),
        nodelay: false,
        tls_enabled: false,
        tls_domain: "localhost".to_string(),
        tls_ca_file: None,
        tls_validate_certificate: false,
    };
    
    let client_wrapper = factory.create_client().await;
    let client = IggyClient::create(client_wrapper, None, None);
    
    // Connect first
    client.connect().await.unwrap();
    
    // Test login as root user
    let identity = client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    
    assert_eq!(identity.user_id, DEFAULT_ROOT_USER_ID);
    
    // Test getting current user info
    let me = client.get_me().await.unwrap();
    assert_eq!(me.user_id, Some(DEFAULT_ROOT_USER_ID));
    
    // Test logout
    client.logout_user().await.unwrap();
    
    server.stop();
}

#[tokio::test]
#[serial] 
async fn should_handle_connection_lifecycle() {
    let mut server = TestServer::new(None, true, None, IpAddrKind::V4);
    server.start();
    
    let factory = TcpClientSyncFactory {
        server_addr: server.get_raw_tcp_addr().unwrap(),
        nodelay: false,
        tls_enabled: false,
        tls_domain: "localhost".to_string(),
        tls_ca_file: None,
        tls_validate_certificate: false,
    };
    
    let client_wrapper = factory.create_client().await;
    let client = IggyClient::create(client_wrapper, None, None);
    
    // Connect first
    client.connect().await.unwrap();
    
    // Test basic ping after connection
    client.ping().await.unwrap();
    
    // Test getting server stats
    let stats = client.get_stats().await.unwrap();
    assert!(stats.clients_count > 0);
    
    server.stop();
}

#[tokio::test]
#[serial]
async fn should_connect_with_tls() {
    let mut server = TestServer::new(
        Some([("IGGY_TCP_TLS_ENABLED".to_string(), "true".to_string())].into()),
        true,
        None,
        IpAddrKind::V4,
    );
    server.start();
    
    let factory = TcpClientSyncFactory {
        server_addr: server.get_raw_tcp_addr().unwrap(),
        nodelay: false,
        tls_enabled: true,
        tls_domain: "localhost".to_string(),
        tls_ca_file: None,
        tls_validate_certificate: false,
    };
    
    let client_wrapper = factory.create_client().await;
    let client = IggyClient::create(client_wrapper, None, None);

    // Connect first
    client.connect().await.unwrap();
    
    // Test basic ping with TLS
    client.ping().await.unwrap();
    
    // Test login with TLS
    client
        .login_user(DEFAULT_ROOT_USERNAME, DEFAULT_ROOT_PASSWORD)
        .await
        .unwrap();
    
    server.stop();
}