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

use std::env;

use iggy_connector_scopedb_sink::{ScopeDBSink, ScopeDBSinkConfig};
use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, Schema, Sink, TopicMetadata};
use scopedb_client::{CatalogOptions, CatalogSchema, Client, Value};
use serde_json::json;
use uuid::Uuid;

#[tokio::test]
#[ignore = "requires SCOPEDB_ENDPOINT and SCOPEDB_API_KEY with catalog creation and read/write permissions"]
async fn given_live_scopedb_when_writing_should_preserve_payloads_and_reject_whole_batches() {
    let endpoint = env::var("SCOPEDB_ENDPOINT").expect("SCOPEDB_ENDPOINT");
    let api_key = env::var("SCOPEDB_API_KEY").expect("SCOPEDB_API_KEY");
    let client = Client::builder(&endpoint)
        .api_key(&api_key)
        .build()
        .expect("ScopeDB client");
    let schema_name = format!("iggy_sink_{}", Uuid::new_v4().simple());
    let definition: CatalogSchema = serde_json::from_value(json!({
        "name": schema_name, "tables": [
            {"name":"mapped", "columns":[
                {"name":"id","data_type":"int"},
                {"name":"iggy_stream","data_type":"string"},
                {"name":"iggy_topic","data_type":"string"},
                {"name":"iggy_partition_id","data_type":"uint"},
                {"name":"iggy_offset","data_type":"uint"},
                {"name":"iggy_timestamp","data_type":"timestamp"},
                {"name":"iggy_id","data_type":"string"}
            ]},
            {"name":"raw", "columns":[{"name":"payload","data_type":"binary"}]}
        ]
    }))
    .expect("catalog definition");
    let resource = client.catalog().schema("scopedb", &schema_name);
    resource
        .apply(&definition, CatalogOptions::default())
        .await
        .expect("create isolated schema");
    let verification = async move {
        let config: ScopeDBSinkConfig = serde_json::from_value(json!({
            "endpoint":endpoint,"api_key":api_key,"schema":schema_name,"table":"mapped", "batch_size":10
        })).expect("sink config");
        let mut sink = ScopeDBSink::new(1, config);
        sink.open()
            .await
            .expect("open mapped sink against real ScopeDB");
        sink.consume(
            &topic(),
            metadata(Schema::Text),
            vec![
                message(10, Payload::Text("{\"id\":1}".into())),
                message(12, Payload::Text("{\"id\":2}".into())),
            ],
        )
        .await
        .expect("write complete mapped request");
        let failure = sink
            .consume(
                &topic(),
                metadata(Schema::Text),
                vec![
                    message(13, Payload::Text("{\"id\":3}".into())),
                    message(14, Payload::Text("{\"id\":\"bad\"}".into())),
                    message(15, Payload::Text("{\"id\":4}".into())),
                ],
            )
            .await;
        assert!(matches!(
            failure,
            Err(iggy_connector_sdk::Error::PermanentHttpError(_))
        ));
        assert_eq!(sink.stats().written, 2);
        assert_eq!(sink.stats().rejected, 0);
        assert_eq!(sink.stats().failed, 3);
        let rows = client
            .query(format!(
                "FROM `scopedb`.`{schema_name}`.`mapped` ORDER BY iggy_offset"
            ))
            .await
            .expect("query mapped rows")
            .into_objects()
            .expect("typed rows");
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0]["id"], Value::Int(1));
        assert_eq!(rows[0]["iggy_offset"], Value::UInt(10));
        assert_eq!(rows[1]["iggy_offset"], Value::UInt(12));
        assert_eq!(rows[0]["iggy_id"], Value::String(u128::MAX.to_string()));
        assert_eq!(rows[0]["iggy_stream"], Value::String("orders".into()));
        sink.close().await.expect("close mapped sink");

        let config: ScopeDBSinkConfig = serde_json::from_value(json!({
            "endpoint":endpoint,"api_key":api_key,"schema":schema_name,"table":"raw",
            "mode":"raw","include_metadata":false
        }))
        .expect("raw sink config");
        let mut sink = ScopeDBSink::new(2, config);
        sink.open()
            .await
            .expect("open raw sink against real ScopeDB");
        sink.consume(
            &topic(),
            metadata(Schema::Raw),
            vec![message(20, Payload::Raw(vec![255, 0, 1]))],
        )
        .await
        .expect("append binary payload");
        let rows = client
            .query(format!("FROM `scopedb`.`{schema_name}`.`raw`"))
            .await
            .expect("query binary row")
            .into_objects()
            .expect("typed binary row");
        assert_eq!(rows.len(), 1);
        let Value::Binary(encoded) = &rows[0]["payload"] else {
            panic!("binary result expected")
        };
        assert_eq!(encoded.to_ascii_lowercase(), "ff0001");
        sink.close().await.expect("close raw sink");
    };
    // Cleanup runs even when an assertion panics in the verification task.
    let verification = tokio::spawn(verification).await;
    let snapshot = resource.get().await.expect("read cleanup catalog version");
    resource
        .delete(CatalogOptions {
            allow_destructive: true,
            expected_version: Some(snapshot.version),
            ..CatalogOptions::default()
        })
        .await
        .expect("remove isolated test schema");
    verification.expect("real ScopeDB verification");
}

fn topic() -> TopicMetadata {
    TopicMetadata {
        stream: "orders".into(),
        topic: "events".into(),
    }
}

fn metadata(schema: Schema) -> MessagesMetadata {
    MessagesMetadata {
        partition_id: 3,
        current_offset: 9999,
        schema,
    }
}

fn message(offset: u64, payload: Payload) -> ConsumedMessage {
    ConsumedMessage {
        id: u128::MAX,
        offset,
        checksum: 0,
        timestamp: 1_700_000_000_000_001,
        origin_timestamp: 0,
        headers: None,
        payload,
    }
}
