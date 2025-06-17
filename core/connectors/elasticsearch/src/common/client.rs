use crate::common::{ConnectorError, ConnectorResult};  
use elasticsearch::{Elasticsearch, http::transport::Transport};  
use std::sync::Arc;  
use tracing::{info, warn};  
  
pub struct ElasticsearchClientManager {  
    client: Arc<Elasticsearch>,  
}  
  
impl ElasticsearchClientManager {  
    pub async fn new(url: &str) -> ConnectorResult<Self> {  
        let transport = Transport::single_node(url)  
            .map_err(|e| ConnectorError::Connection {  
                message: format!("Failed to create transport: {}", e),  
            })?;  
          
        let client = Elasticsearch::new(transport);  
          
        // Test connection  
        match client.ping().send().await {  
            Ok(response) => {  
                if response.status_code().is_success() {  
                    info!("Successfully connected to Elasticsearch at {}", url);  
                } else {  
                    warn!("Elasticsearch ping returned status: {}", response.status_code());  
                }  
            }  
            Err(e) => {  
                return Err(ConnectorError::Connection {  
                    message: format!("Failed to ping Elasticsearch: {}", e),  
                });  
            }  
        }  
          
        Ok(Self {  
            client: Arc::new(client),  
        })  
    }  
      
    pub fn client(&self) -> Arc<Elasticsearch> {  
        self.client.clone()  
    }  
}