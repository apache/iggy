use serde::{Deserialize, Serialize};  
use std::time::Duration;  
  
#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct ElasticsearchSinkConfig {  
    /// Elasticsearch cluster URL  
    pub elasticsearch_url: String,  
      
    /// Index pattern for storing documents (supports date formatting)  
    pub index_pattern: String,  
      
    /// Batch size for bulk operations  
    pub batch_size: usize,  
      
    /// Flush interval for batched writes  
    pub flush_interval: Duration,  
      
    /// Maximum retry attempts for failed operations  
    pub retry_attempts: u32,  
      
    /// Retry interval between attempts  
    pub retry_interval: Duration,  
      
    /// Optional mapping configuration JSON  
    pub mapping_config: Option<String>,  
      
    /// Document ID field (if None, auto-generated)  
    pub document_id_field: Option<String>,  
      
    /// Timestamp field for time-based indices  
    pub timestamp_field: Option<String>,  
      
    /// Enable dead letter queue for failed messages  
    pub enable_dlq: bool,  
      
    /// Dead letter queue topic name  
    pub dlq_topic: Option<String>,  
}  
  
impl Default for ElasticsearchSinkConfig {  
    fn default() -> Self {  
        Self {  
            elasticsearch_url: "http://localhost:9200".to_string(),  
            index_pattern: "iggy-messages-{date}".to_string(),  
            batch_size: 1000,  
            flush_interval: Duration::from_secs(5),  
            retry_attempts: 3,  
            retry_interval: Duration::from_secs(1),  
            mapping_config: None,  
            document_id_field: None,  
            timestamp_field: Some("@timestamp".to_string()),  
            enable_dlq: false,  
            dlq_topic: None,  
        }  
    }  
}  
  
impl ElasticsearchSinkConfig {  
    pub fn validate(&self) -> Result<(), String> {  
        if self.elasticsearch_url.is_empty() {  
            return Err("elasticsearch_url cannot be empty".to_string());  
        }  
          
        if self.index_pattern.is_empty() {  
            return Err("index_pattern cannot be empty".to_string());  
        }  
          
        if self.batch_size == 0 {  
            return Err("batch_size must be greater than 0".to_string());  
        }  
          
        if self.enable_dlq && self.dlq_topic.is_none() {  
            return Err("dlq_topic must be specified when enable_dlq is true".to_string());  
        }  
          
        Ok(())  
    }  
}