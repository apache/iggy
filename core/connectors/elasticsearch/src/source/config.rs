use serde::{Deserialize, Serialize};  
use std::time::Duration;  
  
#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct ElasticsearchSourceConfig {  
    /// Elasticsearch cluster URL  
    pub elasticsearch_url: String,  
      
    /// Elasticsearch query (JSON string)  
    pub query: String,  
      
    /// Index pattern to query from  
    pub index_pattern: String,  
      
    /// Polling interval for checking new data  
    pub poll_interval: Duration,  
      
    /// Batch size for reading documents  
    pub batch_size: usize,  
      
    /// Timestamp field for incremental sync  
    pub timestamp_field: Option<String>,  
      
    /// Scroll timeout for large result sets  
    pub scroll_timeout: Duration,  
      
    /// Maximum number of documents to process per poll  
    pub max_docs_per_poll: Option<usize>,  
      
    /// State storage for tracking last sync position  
    pub state_file_path: Option<String>,  
      
    /// Sort field for consistent ordering  
    pub sort_field: Option<String>,  
}  
  
impl Default for ElasticsearchSourceConfig {  
    fn default() -> Self {  
        Self {  
            elasticsearch_url: "http://localhost:9200".to_string(),  
            query: r#"{"match_all": {}}"#.to_string(),  
            index_pattern: "*".to_string(),  
            poll_interval: Duration::from_secs(10),  
            batch_size: 1000,  
            timestamp_field: Some("@timestamp".to_string()),  
            scroll_timeout: Duration::from_secs(60),  
            max_docs_per_poll: Some(10000),  
            state_file_path: Some("./elasticsearch_source_state.json".to_string()),  
            sort_field: Some("@timestamp".to_string()),  
        }  
    }  
}  
  
impl ElasticsearchSourceConfig {  
    pub fn validate(&self) -> Result<(), String> {  
        if self.elasticsearch_url.is_empty() {  
            return Err("elasticsearch_url cannot be empty".to_string());  
        }  
          
        if self.query.is_empty() {  
            return Err("query cannot be empty".to_string());  
        }  
          
        if self.batch_size == 0 {  
            return Err("batch_size must be greater than 0".to_string());  
        }  
          
        // Validate query is valid JSON  
        if serde_json::from_str::<serde_json::Value>(&self.query).is_err() {  
            return Err("query must be valid JSON".to_string());  
        }  
          
        Ok(())  
    }  
}