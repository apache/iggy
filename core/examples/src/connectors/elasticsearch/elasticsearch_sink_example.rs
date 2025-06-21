use iggy_elasticsearch_connector::{ElasticsearchSink, ElasticsearchSinkConfig};  
use iggy::prelude::*;  
use std::time::Duration;  
use tracing::info;  
  
#[tokio::main]  
async fn main() -> Result<(), Box<dyn std::error::Error>> {  
    tracing_subscriber::init();  
      
    // Create Iggy client  
    let iggy_client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;  
      
    // Configure Elasticsearch Sink  
    let sink_config = ElasticsearchSinkConfig {  
        elasticsearch_url: "http://localhost:9200".to_string(),  
        index_pattern: "iggy-messages-{date}".to_string(),  
        batch_size: 1000,  
        flush_interval: Duration::from_secs(5),  
        retry_attempts: 3,  
        retry_interval: Duration::from_secs(1),  
        mapping_config: Some(r#"{  
            "properties": {  
                "@timestamp": {"type": "date"},  
                "message": {"type": "text"},  
                "level": {"type": "keyword"}  
            }  
        }"#.to_string()),  
        document_id_field: None,  
        timestamp_field: Some("@timestamp".to_string()),  
        enable_dlq: false,  
        dlq_topic: None,  
    };  
      
    // Create and start sink  
    let mut sink = ElasticsearchSink::new(  
        iggy_client,  
        "logs",  
        "application-logs",  
        "elasticsearch-sink-group",  
        sink_config,  
    ).await?;  
      
    info!("Starting Elasticsearch Sink...");  
    sink.start().await?;  
      
    Ok(())  
}