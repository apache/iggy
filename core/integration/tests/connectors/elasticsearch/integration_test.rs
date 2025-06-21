#[cfg(test)]  
mod tests {  
    use super::*;  
    use iggy_elasticsearch_connector::*;  
    use iggy::prelude::*;  
    use std::time::Duration;  
    use tokio::time::sleep;  
      
    #[tokio::test]  
    async fn test_sink_basic_functionality() {  
        // This test requires running Elasticsearch and Iggy instances  
        let iggy_client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")  
            .expect("Failed to create Iggy client");  
          
        let config = ElasticsearchSinkConfig {  
            elasticsearch_url: "http://localhost:9200".to_string(),  
            index_pattern: "test-iggy-{date}".to_string(),  
            batch_size: 10,  
            flush_interval: Duration::from_secs(1),  
            ..Default::default()  
        };  
          
        let mut sink = ElasticsearchSink::new(  
            iggy_client,  
            "test-stream",  
            "test-topic",  
            "test-group",  
            config,  
        ).await.expect("Failed to create sink");  
          
        // Start sink in background  
        tokio::spawn(async move {  
            sink.start().await.expect("Sink failed to start");  
        });  
          
        // Give it time to initialize  
        sleep(Duration::from_secs(2)).await;  
          
        // Test would continue with message production and verification  
        // This is a basic structure - full implementation would require  
        // proper test setup and teardown  
    }  
      
    #[tokio::test]  
    async fn test_source_basic_functionality() {  
        let iggy_client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")  
            .expect("Failed to create Iggy client");  
          
        let config = ElasticsearchSourceConfig {  
            elasticsearch_url: "http://localhost:9200".to_string(),  
            query: r#"{"match_all": {}}"#.to_string(),  
            index_pattern: "test-logs-*".to_string(),  
            poll_interval: Duration::from_secs(5),  
            batch_size: 100,  
            ..Default::default()  
        };  
          
        let mut source = ElasticsearchSource::new(  
            iggy_client,  
            "test-stream",  
            "test-topic",  
            config,  
        ).await.expect("Failed to create source");  
          
        // Start source in background  
        tokio::spawn(async move {  
            source.start().await.expect("Source failed to start");  
        });  
          
        // Give it time to initialize  
        sleep(Duration::from_secs(2)).await;  
          
        // Test would continue with verification of message consumption  
    }  
}