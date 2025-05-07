pub mod file;
pub mod memory;

use std::collections::HashMap;
use crate::core::error::DbError;
use crate::core::types::{Table, Column};

pub trait Storage {
    // 表操作
    fn create_table(&mut self, table: Table) -> Result<(), DbError>;
    fn drop_table(&mut self, table_name: &str) -> Result<(), DbError>;
    fn get_table(&self, table_name: &str) -> Result<Option<&Table>, DbError>;
    fn get_table_mut(&mut self, table_name: &str) -> Result<Option<&mut Table>, DbError>;
    fn list_tables(&self) -> Result<Vec<String>, DbError>;

    // 数据操作
    fn insert_row(&mut self, table_name: &str, row: Vec<crate::core::types::DataType>) -> Result<(), DbError>;
    fn delete_row(&mut self, table_name: &str, row_index: usize) -> Result<(), DbError>;
    fn update_row(&mut self, table_name: &str, row_index: usize, row: Vec<crate::core::types::DataType>) -> Result<(), DbError>;

    // 持久化
    fn save(&self) -> Result<(), DbError>;
    fn load(&mut self) -> Result<(), DbError>;
} 