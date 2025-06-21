use crate::common::{ConnectorError, ConnectorResult, ElasticsearchDocument};  
use crate::sink::ElasticsearchSinkConfig;  
use elasticsearch::{Elasticsearch, BulkParts};  
use serde_json::{json, Value};  
use std::sync::Arc;  
use tracing::{debug, error, info, warn};  
  
pub struct ElasticsearchBulkWriter {  
    client: Arc<Elasticsearch>,  
    config: ElasticsearchSinkConfig,  
}  
  
impl ElasticsearchBulkWriter {  
    pub fn new(client: Arc<Elasticsearch>, config: ElasticsearchSinkConfig) -> Self {  
        Self { client, config }  
    }  
      
    pub async fn write_batch(&self, documents: Vec<ElasticsearchDocument>) -> ConnectorResult<()> {  
        if documents.is_empty() {  
            return Ok(());  
        }  
          
        let mut body: Vec<Value> = Vec::new();  
          
        for doc in documents {  
            let index_name = self.resolve_index_name(&doc)?;  
              
            // Index operation metadata  
            let mut index_meta = json!({  
                "index": {  
                    "_index": index_name  
                }  
            });  
              
            // Add document ID if specified  
            if let Some(id) = &doc.id {  
                index_meta["index"]["_id"] = json!(id);  
            }  
              
            body.push(index_meta);  
            body.push(doc.source);  
        }  
          
        let response = self  
            .client  
            .bulk(BulkParts::None)  
            .body(body)  
            .send()  
            .await?;  
          
        let response_body = response.json::<Value>().await?;  
          
        if let Some(errors) = response_body.get("errors") {  
            if errors.as_bool().unwrap_or(false) {  
                self.handle_bulk_errors(&response_body).await?;  
            }  
        }  
          
        debug!("Successfully wrote {} documents to Elasticsearch", documents.len());  
        Ok(())  
    }  
      
    fn resolve_index_name(&self, doc: &ElasticsearchDocument) -> ConnectorResult<String> {  
        let mut index_name = self.config.index_pattern.clone();  
          
        // Replace date placeholders  
        if index_name.contains("{date}") {  
            let date = if let Some(timestamp) = &doc.timestamp {  
                timestamp.format("%Y.%m.%d").to_string()  
            } else {  
                chrono::Utc::now().format("%Y.%m.%d").to_string()  
            };  
            index_name = index_name.replace("{date}", &date);  
        }  
          
        if index_name.contains("{year}") {  
            let year = if let Some(timestamp) = &doc.timestamp {  
                timestamp.format("%Y").to_string()  
            } else {  
                chrono::Utc::now().format("%Y").to_string()  
            };  
            index_name = index_name.replace("{year}", &year);  
        }  
          
        if index_name.contains("{month}") {  
            let month = if let Some(timestamp) = &doc.timestamp {  
                timestamp.format("%m").to_string()  
            } else {  
                chrono::Utc::now().format("%m").to_string()  
            };  
            index_name = index_name.replace("{month}", &month);  
        }  
          
        Ok(index_name)  
    }  
      
    async fn handle_bulk_errors(&self, response_body: &Value) -> ConnectorResult<()> {  
        if let Some(items) = response_body.get("items").and_then(|i| i.as_array()) {  
            let mut error_count = 0;  
              
            for item in items {  
                if let Some(index_result) = item.get("index") {  
                    if let Some(error) = index_result.get("error") {  
                        error_count += 1;  
                        error!("Bulk index error: {}", error);  
                    }  
                }  
            }  
              
            if error_count > 0 {  
                warn!("Bulk operation completed with {} errors", error_count);  
            }  
        }  
          
        Ok(())  
    }  
      
    pub async fn create_index_template(&self, index_pattern: &str) -> ConnectorResult<()> {  
        if let Some(mapping_config) = &self.config.mapping_config {  
            let template_name = format!("iggy-{}", index_pattern.replace("*", "template"));  
              
            let template_body = json!({  
                "index_patterns": [index_pattern],  
                "template": {  
                    "mappings": serde_json::from_str::<Value>(mapping_config)?  
                }  
            });  
              
            let response = self  
                .client  
                .indices()  
                .put_index_template(elasticsearch::indices::IndicesPutIndexTemplateParts::Name(&template_name))  
                .body(template_body)  
                .send()  
                .await?;  
              
            if response.status_code().is_success() {  
                info!("Created index template: {}", template_name);  
            } else {  
                warn!("Failed to create index template: {}", response.status_code());  
            }  
        }  
          
        Ok(())  
    }  
}
