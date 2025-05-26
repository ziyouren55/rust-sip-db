use thiserror::Error;
use std::io;
use crate::core::types::TypeError;

#[derive(Error, Debug)]
pub enum DbError {
    #[error("IO错误: {0}")]
    IoError(#[from] io::Error),
    
    #[error("序列化错误: {0}")]
    Serialization(String),
    
    #[error("表错误: {0}")]
    TableError(String),
    
    #[error("{0}")]
    TypeError(#[from] TypeError),
    
    #[error("SQL错误: {0}")]
    SqlError(String),
    
    #[error("事务错误: {0}")]
    TransactionError(String),
} 