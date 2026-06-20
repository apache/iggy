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

//! `BrokerAdvertise` parsing and metadata reflection.

use std::net::SocketAddr;

use iggy_gateway_kafka::ServerConfig;
use iggy_gateway_kafka::protocol::api::{API_KEY_METADATA, BrokerAdvertise, handle_request};
use iggy_gateway_kafka::protocol::codec::{Decoder, Encoder};

#[test]
fn default_matches_standard_gateway_port() {
    let b = BrokerAdvertise::default();
    assert_eq!(b.host, "127.0.0.1");
    assert_eq!(b.port, 9093);
}

#[test]
fn metadata_reflects_broker_addr() {
    let broker = BrokerAdvertise {
        host: "203.0.113.7".to_string(),
        port: 9093,
    };
    let mut req = Encoder::with_capacity(4);
    req.write_i32(0);
    let body = handle_request(API_KEY_METADATA, 0, req.freeze(), &broker);

    let mut d = Decoder::new(body);
    assert_eq!(d.read_i32().unwrap(), 1);
    d.read_i32().unwrap();
    let host = d.read_nullable_string().unwrap().unwrap();
    let port = d.read_i32().unwrap();
    assert_eq!(host, "203.0.113.7");
    assert_eq!(port, 9093);
}

#[test]
fn from_server_config_uses_explicit_advertised_host_on_wildcard_bind() {
    let config = ServerConfig {
        bind_addr: "0.0.0.0:9093".to_string(),
        advertised_host: Some("kafka.internal".to_string()),
        ..ServerConfig::default()
    };
    let local_addr: SocketAddr = "0.0.0.0:9093".parse().unwrap();
    let broker = BrokerAdvertise::from_server_config(&config, local_addr).expect("valid config");
    assert_eq!(broker.host, "kafka.internal");
    assert_eq!(broker.port, 9093);
}

#[test]
fn from_server_config_rejects_wildcard_bind_without_advertised_host() {
    let config = ServerConfig {
        bind_addr: "0.0.0.0:9093".to_string(),
        ..ServerConfig::default()
    };
    let local_addr: SocketAddr = "0.0.0.0:9093".parse().unwrap();
    let err = BrokerAdvertise::from_server_config(&config, local_addr).unwrap_err();
    assert!(err.to_string().contains("KAFKA_ADVERTISED_HOST"));
}

#[test]
fn from_server_config_uses_bind_ip_for_non_wildcard_listener() {
    let config = ServerConfig {
        bind_addr: "192.168.1.10:19092".to_string(),
        ..ServerConfig::default()
    };
    let local_addr: SocketAddr = "192.168.1.10:19092".parse().unwrap();
    let broker = BrokerAdvertise::from_server_config(&config, local_addr).expect("valid config");
    assert_eq!(broker.host, "192.168.1.10");
    assert_eq!(broker.port, 19092);
}

#[test]
fn from_server_config_honors_advertised_port_override() {
    let config = ServerConfig {
        bind_addr: "127.0.0.1:9093".to_string(),
        advertised_host: Some("broker.example.com".to_string()),
        advertised_port: Some(19093),
        ..ServerConfig::default()
    };
    let local_addr: SocketAddr = "127.0.0.1:9093".parse().unwrap();
    let broker = BrokerAdvertise::from_server_config(&config, local_addr).expect("valid config");
    assert_eq!(broker.host, "broker.example.com");
    assert_eq!(broker.port, 19093);
}
