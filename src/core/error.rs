use crate::core::types::TypeError;
use std::io;
use thiserror::Error;

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
    
    #[error("Error: Syntax error")]
    SqlError(String),
    
    #[error("事务错误: {0}")]
    TransactionError(String),
}

// 为DbError实现详细错误信息输出
impl DbError {
    // 获取详细的错误信息
    pub fn detailed_message(&self) -> String {
        match self {
            DbError::IoError(err) => format!("IO错误: {}", err),
            DbError::Serialization(msg) => format!("序列化错误: {}", msg),
            DbError::TableError(msg) => format!("表错误: {}", msg),
            DbError::TypeError(err) => format!("{}", err),  // 直接输出原始错误信息
            DbError::SqlError(msg) => format!("SQL语法错误: {}", msg),
            DbError::TransactionError(msg) => format!("事务错误: {}", msg),
        }
    }
    
    // 获取简略的错误信息
    pub fn brief_message(&self) -> String {
        match self {
            DbError::IoError(_) => "Error: IO error".to_string(),
            DbError::Serialization(_) => "Error: Serialization error".to_string(),
            DbError::TableError(_) => "Error: Table error".to_string(),
            DbError::TypeError(err) => format!("{}", err),  // 直接输出原始错误信息，包括主键冲突和字段缺少默认值等错误
            DbError::SqlError(_) => "Error: Syntax error".to_string(),
            DbError::TransactionError(_) => "Error: Transaction error".to_string(),
        }
    }
} 