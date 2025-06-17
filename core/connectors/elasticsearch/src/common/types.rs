use serde::{Deserialize, Serialize};  
use std::collections::HashMap;  
  
#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct ElasticsearchDocument {  
    pub id: Option<String>,  
    pub index: String,  
    pub source: serde_json::Value,  
    pub timestamp: Option<chrono::DateTime<chrono::Utc>>,  
}  
  
#[derive(Debug, Clone, Serialize, Deserialize)]  
pub struct MessageMetadata {  
    pub offset: u64,  
    pub partition_id: u32,  
    pub timestamp: u64,  
    pub headers: HashMap<String, String>,  
}  
  
#[derive(Debug, Clone)]  
pub struct ProcessingStats {  
    pub processed_count: u64,  
    pub error_count: u64,  
    pub last_processed_timestamp: Option<chrono::DateTime<chrono::Utc>>,  
}  
  
impl Default for ProcessingStats {  
    fn default() -> Self {  
        Self {  
            processed_count: 0,  
            error_count: 0,  
            last_processed_timestamp: None,  
        }  
    }  
}