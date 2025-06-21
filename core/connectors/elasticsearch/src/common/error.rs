use thiserror::Error;  
  
#[derive(Error, Debug)]  
pub enum ConnectorError {  
    #[error("Elasticsearch error: {0}")]  
    Elasticsearch(#[from] elasticsearch::Error),  
      
    #[error("Iggy error: {0}")]  
    Iggy(#[from] iggy::error::IggyError),  
      
    #[error("Serialization error: {0}")]  
    Serialization(#[from] serde_json::Error),  
      
    #[error("Configuration error: {message}")]  
    Configuration { message: String },  
      
    #[error("Connection error: {message}")]  
    Connection { message: String },  
      
    #[error("Processing error: {message}")]  
    Processing { message: String },  
      
    #[error("IO error: {0}")]  
    Io(#[from] std::io::Error),  
}  
  
pub type ConnectorResult<T> = Result<T, ConnectorError>;