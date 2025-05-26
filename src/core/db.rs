use std::path::PathBuf;
use crate::core::error::DbError;
use crate::core::storage::{Storage, file::FileStorage, memory::MemoryStorage};
use crate::core::types::{Table, Column, DataType};
use crate::core::transaction::Transaction;
use crate::core::sql::{SqlParser, SqlExecutor, SqlStatement};

pub enum StorageType {
    File(PathBuf),
    Memory,
}

// 错误显示模式
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ErrorDisplayMode {
    Brief,    // 简略错误信息
    Detailed, // 详细错误信息
}

pub struct Database {
    storage: Box<dyn Storage>,
    sql_parser: SqlParser,
    error_mode: ErrorDisplayMode, // 错误显示模式
}

impl Database {
    pub fn new(storage_type: StorageType) -> Self {
        let storage: Box<dyn Storage> = match storage_type {
            StorageType::File(path) => Box::new(FileStorage::new(path)),
            StorageType::Memory => Box::new(MemoryStorage::new()),
        };
        
        Database { 
            storage,
            sql_parser: SqlParser::new(),
            error_mode: ErrorDisplayMode::Brief, // 默认使用简略模式
        }
    }
    
    // 设置错误显示模式
    pub fn set_error_mode(&mut self, mode: ErrorDisplayMode) {
        self.error_mode = mode;
    }
    
    // 获取当前错误显示模式
    pub fn get_error_mode(&self) -> ErrorDisplayMode {
        self.error_mode
    }
    
    // 切换错误显示模式
    pub fn toggle_error_mode(&mut self) -> ErrorDisplayMode {
        self.error_mode = match self.error_mode {
            ErrorDisplayMode::Brief => ErrorDisplayMode::Detailed,
            ErrorDisplayMode::Detailed => ErrorDisplayMode::Brief,
        };
        self.error_mode
    }
    
    // 根据当前模式格式化错误信息
    pub fn format_error(&self, error: &DbError) -> String {
        match self.error_mode {
            ErrorDisplayMode::Brief => error.brief_message(),
            ErrorDisplayMode::Detailed => error.detailed_message(),
        }
    }

    // SQL操作
    pub fn execute_sql(&mut self, sql: &str) -> Result<(), DbError> {
        let statement = self.sql_parser.parse(sql)?;
        let mut executor = SqlExecutor::new(&mut *self.storage);
        executor.execute(statement)
    }
    
    // 执行SQL并返回是否有输出
    pub fn execute_sql_with_output(&mut self, sql: &str) -> Result<bool, DbError> {
        let statement = self.sql_parser.parse(sql)?;
        let mut executor = SqlExecutor::new(&mut *self.storage);
        executor.execute(statement)?;
        Ok(executor.has_output())
    }

    // 表操作
    pub fn create_table(&mut self, name: String, columns: Vec<Column>) -> Result<(), DbError> {
        let table = Table::new(name, columns);
        self.storage.create_table(table)
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), DbError> {
        self.storage.drop_table(table_name)
    }

    pub fn get_table(&self, table_name: &str) -> Result<Option<&Table>, DbError> {
        self.storage.get_table(table_name)
    }

    pub fn list_tables(&self) -> Result<Vec<String>, DbError> {
        self.storage.list_tables()
    }

    // 数据操作
    pub fn insert_row(&mut self, table_name: &str, row: Vec<DataType>) -> Result<(), DbError> {
        self.storage.insert_row(table_name, row)
    }

    pub fn delete_row(&mut self, table_name: &str, row_index: usize) -> Result<(), DbError> {
        self.storage.delete_row(table_name, row_index)
    }

    pub fn update_row(&mut self, table_name: &str, row_index: usize, row: Vec<DataType>) -> Result<(), DbError> {
        self.storage.update_row(table_name, row_index, row)
    }

    // 持久化
    pub fn save(&self) -> Result<(), DbError> {
        self.storage.save()
    }

    pub fn load(&mut self) -> Result<(), DbError> {
        self.storage.load()
    }

    // 事务
    pub fn begin_transaction(&mut self) -> Transaction {
        Transaction::new(&mut *self.storage)
    }
} 