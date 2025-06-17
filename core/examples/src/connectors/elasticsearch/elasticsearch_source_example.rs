use iggy_elasticsearch_connector::{ElasticsearchSource, ElasticsearchSourceConfig};  
use iggy::prelude::*;  
use std::time::Duration;  
use tracing::info;  
  
#[tokio::main]  
async fn main() -> Result<(), Box<dyn std::error::Error>> {  
    tracing_subscriber::init();  
      
    // Create Iggy client  
    let iggy_client = IggyClient::from_connection_string("iggy://iggy:iggy@localhost:8090")?;  
      
    // Configure Elasticsearch Source  
    let source_config = ElasticsearchSourceConfig {  
        elasticsearch_url: "http://localhost:9200".to_string(),  
        query: r#"{  
            "bool": {  
                "must": [  
                    {"range": {"@timestamp": {"gte": "now-1h"}}}  
                ]  
            }  
        }"#.to_string(),  
        index_pattern: "logs-*".to_string(),  
        poll_interval: Duration::from_secs(30),  
        batch_size: 500,  
        timestamp_field: Some("@timestamp".to_string()),  
        scroll_timeout: Duration::from_secs(60),  
        max_docs_per_poll: Some(5000),  
        state_file_path: Some("./elasticsearch_source_state.json".to_string()),  
        sort_field: Some("@timestamp".to_string()),  
    };  
      
    // Create and start source  
    let mut source = ElasticsearchSource::new(  
        iggy_client,  
        "external-logs",  
        "imported-logs",  
        source_config,  
    ).await?;  
      
    info!("Starting Elasticsearch Source...");  
    source.start().await?;  
      
    Ok(())  
}