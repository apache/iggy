use crate::common::{ConnectorError, ConnectorResult, ElasticsearchClientManager, ProcessingStats};  
use crate::source::{ElasticsearchSourceConfig, reader::ElasticsearchReader};  
use iggy::prelude::*;  
use std::sync::Arc;  
use std::time::Duration;  
use tokio::sync::{mpsc, Mutex};  
use tokio::time::{interval, timeout};  
use tracing::{debug, error, info, warn};  
  
pub struct ElasticsearchSource {  
    producer: IggyProducer,  
    reader: ElasticsearchReader,  
    config: ElasticsearchSourceConfig,  
    stats: Arc<Mutex<ProcessingStats>>,  
    shutdown_tx: Option<mpsc::Sender<()>>,  
}  
  
impl ElasticsearchSource {  
    pub async fn new(  
        iggy_client: IggyClient,  
        stream_name: &str,  
        topic_name: &str,  
        config: ElasticsearchSourceConfig,  
    ) -> ConnectorResult<Self> {  
        config.validate().map_err(|e| ConnectorError::Configuration { message: e })?;  
          
        // Create Elasticsearch client  
        let es_client_manager = ElasticsearchClientManager::new(&config.elasticsearch_url).await?;  
        let mut reader = ElasticsearchReader::new(es_client_manager.client(), config.clone());  
          
        // Load previous state  
        reader.load_state().await?;  
          
        // Create Iggy producer with proper configuration  
        let producer = iggy_client  
            .producer(stream_name, topic_name)?  
            .create_stream_if_not_exists()  
            .create_topic_if_not_exists()  
            .batch_size(config.batch_size as u32)  
            .send_interval(IggyDuration::from_str("1ms")?)  
            .partitioning(Partitioning::balanced())  
            .build();  
          
        Ok(Self {  
            producer,  
            reader,  
            config,  
            stats: Arc::new(Mutex::new(ProcessingStats::default())),  
            shutdown_tx: None,  
        })  
    }  
      
    pub async fn start(&mut self) -> ConnectorResult<()> {  
        info!("Starting Elasticsearch Source connector");  
          
        // Initialize producer  
        self.producer.init().await?;  
          
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);  
        self.shutdown_tx = Some(shutdown_tx);  
          
        // Start polling timer  
        let poll_interval = self.config.poll_interval;  
        let mut poll_timer = interval(poll_interval);  
          
        loop {  
            tokio::select! {  
                _ = poll_timer.tick() => {  
                    if let Err(e) = self.poll_and_send().await {  
                        error!("Failed to poll and send data: {}", e);  
                        self.update_error_stats().await;  
                    }  
                }  
                _ = shutdown_rx.recv() => {  
                    info!("Received shutdown signal");  
                    break;  
                }  
            }  
        }  
          
        Ok(())  
    }  
      
    async fn poll_and_send(&mut self) -> ConnectorResult<()> {  
        debug!("Polling Elasticsearch for new documents");  
          
        let documents = self.reader.read_documents().await?;  
          
        if documents.is_empty() {  
            debug!("No new documents found");  
            return Ok(());  
        }  
          
        info!("Found {} new documents to send", documents.len());  
          
        // Convert documents to Iggy messages and send in batches  
        let mut messages = Vec::new();  
        for document in documents {  
            let message = self.convert_document_to_message(document)?;  
            messages.push(message);  
              
            // Send in batches to avoid memory issues  
            if messages.len() >= self.config.batch_size {  
                self.send_message_batch(messages.clone()).await?;  
                messages.clear();  
            }  
        }  
          
        // Send remaining messages  
        if !messages.is_empty() {  
            self.send_message_batch(messages).await?;  
        }  
          
        // Save state after successful processing  
        self.reader.save_state().await?;  
          
        Ok(())  
    }  
      
    async fn send_message_batch(&self, messages: Vec<IggyMessage>) -> ConnectorResult<()> {  
        match timeout(Duration::from_secs(30), self.producer.send(messages.clone())).await {  
            Ok(Ok(_)) => {  
                debug!("Successfully sent batch of {} messages", messages.len());  
                for _ in &messages {  
                    self.update_processed_stats().await;  
                }  
                Ok(())  
            }  
            Ok(Err(e)) => {  
                error!("Failed to send message batch: {}", e);  
                for _ in &messages {  
                    self.update_error_stats().await;  
                }  
                Err(ConnectorError::Iggy(e))  
            }  
            Err(_) => {  
                error!("Timeout sending message batch");  
                for _ in &messages {  
                    self.update_error_stats().await;  
                }  
                Err(ConnectorError::Processing {  
                    message: "Timeout sending message batch".to_string(),  
                })  
            }  
        }  
    }  
      
    fn convert_document_to_message(&self, document: crate::common::ElasticsearchDocument) -> ConnectorResult<IggyMessage> {  
        let payload = serde_json::to_vec(&document.source)?;  
          
        let mut headers = std::collections::HashMap::new();  
        headers.insert("source_index".to_string(), document.index);  
          
        if let Some(id) = document.id {  
            headers.insert("document_id".to_string(), id);  
        }  
          
        if let Some(timestamp) = document.timestamp {  
            headers.insert("source_timestamp".to_string(), timestamp.to_rfc3339());  
        }  
          
        // Create IggyMessage using the builder pattern  
        let message = IggyMessage::builder()  
            .id(uuid::Uuid::new_v4().as_u128())  
            .payload(payload.into())  
            .user_headers(headers)  
            .build()  
            .map_err(|e| ConnectorError::Processing {  
                message: format!("Failed to create IggyMessage: {}", e),  
            })?;  
          
        Ok(message)  
    }  
      
    async fn update_processed_stats(&self) {  
        let mut stats = self.stats.lock().await;  
        stats.processed_count += 1;  
        stats.last_processed_timestamp = Some(chrono::Utc::now());  
    }  
      
    async fn update_error_stats(&self) {  
        let mut stats = self.stats.lock().await;  
        stats.error_count += 1;  
    }  
      
    pub async fn get_stats(&self) -> ProcessingStats {  
        self.stats.lock().await.clone()  
    }  
      
    pub async fn shutdown(&mut self) -> ConnectorResult<()> {  
        info!("Shutting down Elasticsearch Source connector");  
          
        // Signal shutdown  
        if let Some(tx) = self.shutdown_tx.take() {  
            let _ = tx.send(()).await;  
        }  
          
        // Save final state  
        self.reader.save_state().await?;  
          
        info!("Elasticsearch Source connector shutdown complete");  
        Ok(())  
    }  
}