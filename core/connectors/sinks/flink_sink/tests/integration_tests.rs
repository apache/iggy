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

#[cfg(test)]
mod tests {
    // Note: The FlinkSink struct is not publicly exposed, so these are basic tests
    // Full integration tests would require the struct to be public or tests in lib.rs

    use iggy_connector_sdk::{ConsumedMessage, MessagesMetadata, Payload, Schema, TopicMetadata};
    use serde_json::json;

    #[tokio::test]
    async fn test_mockito_server() {
        // Basic test to ensure mockito server works
        let mut server = mockito::Server::new_async().await;
        let _m = server
            .mock("GET", "/v1/overview")
            .with_status(200)
            .with_body(
                json!({
                    "version": "1.18.0",
                    "commit": "abc123"
                })
                .to_string(),
            )
            .create_async()
            .await;

        let response = reqwest::get(&format!("{}/v1/overview", server.url()))
            .await
            .unwrap();

        assert_eq!(response.status(), 200);
    }

    #[tokio::test]
    async fn test_message_conversion() {
        // Test that we can create valid ConsumedMessage structures
        let json_str = r#"{"field": "value"}"#;
        let mut json_bytes = json_str.as_bytes().to_vec();
        let simd_value = simd_json::to_owned_value(&mut json_bytes).unwrap();

        let message = ConsumedMessage {
            id: 1,
            offset: 0,
            checksum: 12345,
            timestamp: 1000000,
            origin_timestamp: 1000000,
            headers: None,
            payload: Payload::Json(simd_value),
        };

        assert_eq!(message.id, 1);
        assert_eq!(message.offset, 0);
    }

    #[tokio::test]
    async fn test_metadata_creation() {
        let topic_meta = TopicMetadata {
            stream: "test-stream".to_string(),
            topic: "test-topic".to_string(),
        };

        let messages_meta = MessagesMetadata {
            partition_id: 1,
            current_offset: 100,
            schema: Schema::Json,
        };

        assert_eq!(topic_meta.stream, "test-stream");
        assert_eq!(messages_meta.partition_id, 1);
        assert_eq!(messages_meta.schema, Schema::Json);
    }

    // The actual connector tests would be done via the runtime
    // when loading the dynamic library
}
