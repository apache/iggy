use crate::common::{ConnectorError, ConnectorResult, ElasticsearchClientManager, ElasticsearchDocument, ProcessingStats};  
use crate::sink::{ElasticsearchSinkConfig, writer::ElasticsearchBulkWriter};  
use iggy::prelude::*;  
use std::sync::Arc;  
use std::time::Duration;  
use tokio::sync::{mpsc, Mutex};  
use tokio::time::{interval, timeout};  
use tracing::{debug, error, info, warn};  
  
pub struct ElasticsearchSink {  
    consumer: IggyConsumer,  
    writer: ElasticsearchBulkWriter,  
    config: ElasticsearchSinkConfig,  
    stats: Arc<Mutex<ProcessingStats>>,  
    shutdown_tx: Option<mpsc::Sender<()>>,  
    batch_buffer: Arc<Mutex<Vec<ElasticsearchDocument>>>,  
}  
  
impl ElasticsearchSink {  
    pub async fn new(  
        iggy_client: IggyClient,  
        stream_name: &str,  
        topic_name: &str,  
        consumer_group_name: &str,  
        config: ElasticsearchSinkConfig,  
    ) -> ConnectorResult<Self> {  
        config.validate().map_err(|e| ConnectorError::Configuration { message: e })?;  
          
        // Create Elasticsearch client  
        let es_client_manager = ElasticsearchClientManager::new(&config.elasticsearch_url).await?;  
        let writer = ElasticsearchBulkWriter::new(es_client_manager.client(), config.clone());  
          
        // Create Iggy consumer with proper configuration  
        let consumer = iggy_client  
            .consumer_group(consumer_group_name, stream_name, topic_name)?  
            .auto_commit(AutoCommit::IntervalOrWhen(  
                IggyDuration::from_str("5s")?,  
                AutoCommitWhen::ConsumingAllMessages,  
            ))  
            .create_consumer_group_if_not_exists()  
            .auto_join_consumer_group()  
            .polling_strategy(PollingStrategy::next())  
            .poll_interval(IggyDuration::from_str("100ms")?)  
            .batch_size(config.batch_size as u32)  
            .build();  
          
        Ok(Self {  
            consumer,  
            writer,  
            config,  
            stats: Arc::new(Mutex::new(ProcessingStats::default())),  
            shutdown_tx: None,  
            batch_buffer: Arc::new(Mutex::new(Vec::new())),  
        })  
    }  
      
    pub async fn start(&mut self) -> ConnectorResult<()> {  
        info!("Starting Elasticsearch Sink connector");  
          
        // Initialize consumer  
        self.consumer.init().await?;  
          
        // Create index template if configured  
        if self.config.mapping_config.is_some() {  
            self.writer.create_index_template(&self.config.index_pattern).await?;  
        }  
          
        let (shutdown_tx, mut shutdown_rx) = mpsc::channel::<()>(1);  
        self.shutdown_tx = Some(shutdown_tx);  
          
        // Start flush timer  
        let flush_interval = self.config.flush_interval;  
        let batch_buffer = self.batch_buffer.clone();  
        let writer = self.writer.clone();  
        let stats = self.stats.clone();  
          
        tokio::spawn(async move {  
            let mut flush_timer = interval(flush_interval);  
            loop {  
                tokio::select! {  
                    _ = flush_timer.tick() => {  
                        if let Err(e) = Self::flush_batch(&batch_buffer, &writer, &stats).await {  
                            error!("Failed to flush batch: {}", e);  
                        }  
                    }  
                    _ = shutdown_rx.recv() => {  
                        info!("Flush timer shutting down");  
                        break;  
                    }  
                }  
            }  
        });  
          
        // Main processing loop  
        self.process_messages().await  
    }  
      
    async fn process_messages(&mut self) -> ConnectorResult<()> {  
        info!("Starting message processing loop");  
          
        loop {  
            match timeout(Duration::from_secs(30), self.consumer.next()).await {  
                Ok(Some(message)) => {  
                    if let Err(e) = self.process_message(message).await {  
                        error!("Failed to process message: {}", e);  
                        self.update_error_stats().await;  
                    }  
                }  
                Ok(None) => {  
                    debug!("No messages available, continuing...");  
                    tokio::time::sleep(Duration::from_millis(100)).await;  
                }  
                Err(_) => {  
                    warn!("Message polling timeout, continuing...");  
                }  
            }  
        }  
    }  
      
    async fn process_message(&self, message: IggyMessage) -> ConnectorResult<()> {  
        let document = self.convert_message_to_document(message)?;  
          
        let mut buffer = self.batch_buffer.lock().await;  
        buffer.push(document);  
          
        if buffer.len() >= self.config.batch_size {  
            let documents = buffer.drain(..).collect();  
            drop(buffer);  
              
            self.write_batch_with_retry(documents).await?;  
        }  
          
        self.update_processed_stats().await;  
        Ok(())  
    }  
      
    fn convert_message_to_document(&self, message: IggyMessage) -> ConnectorResult<ElasticsearchDocument> {  
        let payload_str = String::from_utf8(message.payload.to_vec())  
            .map_err(|e| ConnectorError::Processing {  
                message: format!("Failed to convert payload to string: {}", e),  
            })?;  
          
        let mut source: serde_json::Value = serde_json::from_str(&payload_str)  
            .unwrap_or_else(|_| serde_json::json!({ "raw_message": payload_str }));  
          
        // Add timestamp if configured  
        if let Some(timestamp_field) = &self.config.timestamp_field {  
            let timestamp = chrono::DateTime::from_timestamp_micros(message.timestamp as i64)  
                .unwrap_or_else(chrono::Utc::now);  
            source[timestamp_field] = serde_json::json!(timestamp.to_rfc3339());  
        }  
          
        // Extract document ID if configured  
        let document_id = if let Some(id_field) = &self.config.document_id_field {  
            source.get(id_field).and_then(|v| v.as_str()).map(String::from)  
        } else {  
            Some(uuid::Uuid::new_v4().to_string())  
        };  
          
        Ok(ElasticsearchDocument {  
            id: document_id,  
            index: self.config.index_pattern.clone(),  
            source,  
            timestamp: Some(chrono::DateTime::from_timestamp_micros(message.timestamp as i64)  
                .unwrap_or_else(chrono::Utc::now)),  
        })  
    }  
      
    async fn write_batch_with_retry(&self, documents: Vec<ElasticsearchDocument>) -> ConnectorResult<()> {  
        let mut attempts = 0;  
        let max_attempts = self.config.retry_attempts;  
          
        while attempts < max_attempts {  
            match self.writer.write_batch(documents.clone()).await {  
                Ok(()) => {  
                    debug!("Successfully wrote batch of {} documents", documents.len());  
                    return Ok(());  
                }  
                Err(e) => {  
                    attempts += 1;  
                    error!("Batch write attempt {} failed: {}", attempts, e);  
                      
                    if attempts < max_attempts {  
                        tokio::time::sleep(self.config.retry_interval).await;  
                    } else {  
                        if self.config.enable_dlq {  
                            self.send_to_dlq(documents).await?;  
                        }  
                        return Err(e);  
                    }  
                }  
            }  
        }  
          
        Ok(())  
    }  
      
    async fn send_to_dlq(&self, documents: Vec<ElasticsearchDocument>) -> ConnectorResult<()> {  
        if let Some(dlq_topic) = &self.config.dlq_topic {  
            warn!("Sending {} documents to DLQ topic: {}", documents.len(), dlq_topic);  
            // DLQ implementation would go here  
        }  
        Ok(())  
    }  
      
    async fn flush_batch(  
        batch_buffer: &Arc<Mutex<Vec<ElasticsearchDocument>>>,  
        writer: &ElasticsearchBulkWriter,  
        stats: &Arc<Mutex<ProcessingStats>>,  
    ) -> ConnectorResult<()> {  
        let mut buffer = batch_buffer.lock().await;  
        if buffer.is_empty() {  
            return Ok(());  
        }  
          
        let documents = buffer.drain(..).collect::<Vec<_>>();  
        drop(buffer);  
          
        debug!("Flushing batch of {} documents", documents.len());  
        writer.write_batch(documents).await?;  
          
        let mut stats = stats.lock().await;  
        stats.last_processed_timestamp = Some(chrono::Utc::now());  
          
        Ok(())  
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
        info!("Shutting down Elasticsearch Sink connector");  
          
        if let Some(tx) = self.shutdown_tx.take() {  
            let _ = tx.send(()).await;  
        }  
          
        Self::flush_batch(&self.batch_buffer, &self.writer, &self.stats).await?;  
          
        info!("Elasticsearch Sink connector shutdown complete");  
        Ok(())  
    }  
}