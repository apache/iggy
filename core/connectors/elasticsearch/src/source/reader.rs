use crate::common::{ConnectorError, ConnectorResult, ElasticsearchDocument};  
use crate::source::ElasticsearchSourceConfig;  
use elasticsearch::{Elasticsearch, SearchParts, ScrollParts};  
use serde_json::{json, Value};  
use std::sync::Arc;  
use tracing::{debug, error, info, warn};  
  
pub struct ElasticsearchReader {  
    client: Arc<Elasticsearch>,  
    config: ElasticsearchSourceConfig,  
    last_sync_timestamp: Option<chrono::DateTime<chrono::Utc>>,  
}  
  
impl ElasticsearchReader {  
    pub fn new(client: Arc<Elasticsearch>, config: ElasticsearchSourceConfig) -> Self {  
        Self {  
            client,  
            config,  
            last_sync_timestamp: None,  
        }  
    }  
      
    pub async fn read_documents(&mut self) -> ConnectorResult<Vec<ElasticsearchDocument>> {  
        let query = self.build_incremental_query()?;  
        let mut all_documents = Vec::new();  
        let mut processed_count = 0;  
          
        // Initial search  
        let response = self  
            .client  
            .search(SearchParts::Index(&[&self.config.index_pattern]))  
            .body(query)  
            .scroll(&format!("{}s", self.config.scroll_timeout.as_secs()))  
            .size(self.config.batch_size as i64)  
            .send()  
            .await?;  
          
        let mut response_body = response.json::<Value>().await?;  
        let mut scroll_id = response_body  
            .get("_scroll_id")  
            .and_then(|v| v.as_str())  
            .map(String::from);  
          
        // Process initial batch  
        if let Some(hits) = response_body.get("hits").and_then(|h| h.get("hits")).and_then(|h| h.as_array()) {  
            for hit in hits {  
                if let Some(doc) = self.convert_hit_to_document(hit)? {  
                    all_documents.push(doc);  
                    processed_count += 1;  
                      
                    if let Some(max_docs) = self.config.max_docs_per_poll {  
                        if processed_count >= max_docs {  
                            break;  
                        }  
                    }  
                }  
            }  
        }  
          
        // Continue scrolling if we have more data and haven't reached the limit  
        while let Some(scroll_id_value) = scroll_id {  
            if let Some(max_docs) = self.config.max_docs_per_poll {  
                if processed_count >= max_docs {  
                    break;  
                }  
            }  
              
            let scroll_response = self  
                .client  
                .scroll(ScrollParts::ScrollId(&scroll_id_value))  
                .scroll(&format!("{}s", self.config.scroll_timeout.as_secs()))  
                .send()  
                .await?;  
              
            response_body = scroll_response.json::<Value>().await?;  
            scroll_id = response_body  
                .get("_scroll_id")  
                .and_then(|v| v.as_str())  
                .map(String::from);  
              
            if let Some(hits) = response_body.get("hits").and_then(|h| h.get("hits")).and_then(|h| h.as_array()) {  
                if hits.is_empty() {  
                    break;  
                }  
                  
                for hit in hits {  
                    if let Some(doc) = self.convert_hit_to_document(hit)? {  
                        all_documents.push(doc);  
                        processed_count += 1;  
                          
                        if let Some(max_docs) = self.config.max_docs_per_poll {  
                            if processed_count >= max_docs {  
                                break;  
                            }  
                        }  
                    }  
                }  
            } else {  
                break;  
            }  
        }  
          
        // Update last sync timestamp  
        if !all_documents.is_empty() {  
            if let Some(last_doc) = all_documents.last() {  
                self.last_sync_timestamp = last_doc.timestamp;  
            }  
        }  
          
        debug!("Read {} documents from Elasticsearch", all_documents.len());  
        Ok(all_documents)  
    }  
      
    fn build_incremental_query(&self) -> ConnectorResult<Value> {  
        let mut base_query: Value = serde_json::from_str(&self.config.query)?;  
          
        // Add timestamp filter for incremental sync  
        if let Some(timestamp_field) = &self.config.timestamp_field {  
            if let Some(last_timestamp) = &self.last_sync_timestamp {  
                let range_filter = json!({  
                    "range": {  
                        timestamp_field: {  
                            "gt": last_timestamp.to_rfc3339()  
                        }  
                    }  
                });  
                  
                // Wrap existing query in bool query if needed  
                if base_query.get("bool").is_none() {  
                    base_query = json!({  
                        "bool": {  
                            "must": [base_query]  
                        }  
                    });  
                }  
                  
                if let Some(bool_query) = base_query.get_mut("bool") {  
                    if let Some(filter_array) = bool_query.get_mut("filter") {  
                        if let Some(filters) = filter_array.as_array_mut() {  
                            filters.push(range_filter);  
                        }  
                    } else {  
                        bool_query["filter"] = json!([range_filter]);  
                    }  
                }  
            }  
        }  
          
        // Add sort if configured  
        let mut query_body = json!({  
            "query": base_query  
        });  
          
        if let Some(sort_field) = &self.config.sort_field {  
            query_body["sort"] = json!([{  
                sort_field: {  
                    "order": "asc"  
                }  
            }]);  
        }  
          
        Ok(query_body)  
    }  
      
    fn convert_hit_to_document(&self, hit: &Value) -> ConnectorResult<Option<ElasticsearchDocument>> {  
        let source = hit.get("_source").cloned().unwrap_or_default();  
        let index = hit.get("_index").and_then(|v| v.as_str()).unwrap_or("unknown").to_string();  
        let id = hit.get("_id").and_then(|v| v.as_str()).map(String::from);  
          
        // Extract timestamp if configured  
        let timestamp = if let Some(timestamp_field) = &self.config.timestamp_field {  
            source.get(timestamp_field)  
                .and_then(|v| v.as_str())  
                .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())  
                .map(|dt| dt.with_timezone(&chrono::Utc))  
        } else {  
            None  
        };  
          
        Ok(Some(ElasticsearchDocument {  
            id,  
            index,  
            source,  
            timestamp,  
        }))  
    }  
      
    pub async fn load_state(&mut self) -> ConnectorResult<()> {  
        if let Some(state_file) = &self.config.state_file_path {  
            match tokio::fs::read_to_string(state_file).await {  
                Ok(content) => {  
                    if let Ok(state) = serde_json::from_str::<Value>(&content) {  
                        if let Some(timestamp_str) = state.get("last_sync_timestamp").and_then(|v| v.as_str()) {  
                            if let Ok(timestamp) = chrono::DateTime::parse_from_rfc3339(timestamp_str) {  
                                self.last_sync_timestamp = Some(timestamp.with_timezone(&chrono::Utc));  
                                info!("Loaded state: last sync timestamp = {}", timestamp);  
                            }  
                        }  
                    }  
                }  
                Err(_) => {  
                    info!("No existing state file found, starting fresh sync");  
                }  
            }  
        }  
        Ok(())  
    }  
      
    pub async fn save_state(&self) -> ConnectorResult<()> {  
        if let Some(state_file) = &self.config.state_file_path {  
            let state = json!({  
                "last_sync_timestamp": self.last_sync_timestamp.map(|t| t.to_rfc3339())  
            });  
              
            tokio::fs::write(state_file, serde_json::to_string_pretty(&state)?).await?;  
            debug!("Saved state to {}", state_file);  
        }  
        Ok(())  
    }  
}