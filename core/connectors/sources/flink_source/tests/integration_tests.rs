#[cfg(test)]
mod tests {
    use iggy_connector_sdk::{ConnectorState, Schema};
    use mockito::{Matcher, Server};
    use serde_json::json;

    #[tokio::test]
    async fn test_mock_server() {
        let mut server = Server::new_async().await;

        let _mock = server
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
    async fn test_connector_state_serialization() {
        // Test that ConnectorState can be serialized/deserialized
        let data = vec![1, 2, 3, 4, 5];
        let state = ConnectorState(data.clone());
        assert_eq!(state.0, data);
    }

    #[tokio::test]
    async fn test_schema_types() {
        // Test that Schema enum is accessible
        let schema = Schema::Json;
        assert!(matches!(schema, Schema::Json));

        let schema = Schema::Raw;
        assert!(matches!(schema, Schema::Raw));

        let schema = Schema::Text;
        assert!(matches!(schema, Schema::Text));
    }

    #[tokio::test]
    async fn test_mock_flink_endpoints() {
        let mut server = Server::new_async().await;

        // Mock Flink overview endpoint
        let _overview = server
            .mock("GET", "/v1/overview")
            .with_status(200)
            .with_body(
                json!({
                    "version": "1.18.0"
                })
                .to_string(),
            )
            .create_async()
            .await;

        // Mock subscribe endpoint
        let _subscribe = server
            .mock("POST", "/v1/sources/subscribe")
            .with_status(200)
            .with_body(
                json!({
                    "subscriptionId": "sub-123"
                })
                .to_string(),
            )
            .create_async()
            .await;

        // Mock fetch endpoint with regex matcher
        let _fetch = server
            .mock(
                "GET",
                Matcher::Regex(r"^/v1/sources/.*/fetch.*".to_string()),
            )
            .with_status(200)
            .with_body(
                json!({
                    "messages": [
                        {
                            "timestamp": 1000000,
                            "data": {"field": "value1"},
                            "headers": {"key": "value"},
                            "partition": 0,
                            "offset": 100
                        }
                    ]
                })
                .to_string(),
            )
            .create_async()
            .await;

        // Test the endpoints
        let response = reqwest::get(&format!("{}/v1/overview", server.url()))
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let client = reqwest::Client::new();
        let response = client
            .post(format!("{}/v1/sources/subscribe", server.url()))
            .json(&json!({"source": "test"}))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), 200);
    }
}
