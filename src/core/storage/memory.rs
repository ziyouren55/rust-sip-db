use std::collections::HashMap;
use crate::core::error::DbError;
use crate::core::types::{Table, DataType};
use super::Storage;

pub struct MemoryStorage {
    tables: HashMap<String, Table>,
}

impl MemoryStorage {
    pub fn new() -> Self {
        MemoryStorage {
            tables: HashMap::new(),
        }
    }
}

impl Storage for MemoryStorage {
    fn create_table(&mut self, table: Table) -> Result<(), DbError> {
        if self.tables.contains_key(&table.name) {
            return Err(DbError::TableError(format!("表 {} 已存在", table.name)));
        }
        self.tables.insert(table.name.clone(), table);
        Ok(())
    }

    fn drop_table(&mut self, table_name: &str) -> Result<(), DbError> {
        if !self.tables.contains_key(table_name) {
            return Err(DbError::TableError(format!("表 {} 不存在", table_name)));
        }
        self.tables.remove(table_name);
        Ok(())
    }

    fn get_table(&self, table_name: &str) -> Result<Option<&Table>, DbError> {
        Ok(self.tables.get(table_name))
    }

    fn get_table_mut(&mut self, table_name: &str) -> Result<Option<&mut Table>, DbError> {
        Ok(self.tables.get_mut(table_name))
    }

    fn list_tables(&self) -> Result<Vec<String>, DbError> {
        Ok(self.tables.keys().cloned().collect())
    }

    fn insert_row(&mut self, table_name: &str, row: Vec<DataType>) -> Result<(), DbError> {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        table.insert_row(row)?;
        Ok(())
    }

    fn delete_row(&mut self, table_name: &str, row_index: usize) -> Result<(), DbError> {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        if row_index >= table.rows.len() {
            return Err(DbError::TableError(format!("行索引 {} 超出范围", row_index)));
        }
        table.rows.remove(row_index);
        Ok(())
    }

    fn update_row(&mut self, table_name: &str, row_index: usize, row: Vec<DataType>) -> Result<(), DbError> {
        let table = self.tables.get_mut(table_name)
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        if row_index >= table.rows.len() {
            return Err(DbError::TableError(format!("行索引 {} 超出范围", row_index)));
        }
        table.validate_row(&row)?;
        table.rows[row_index] = row;
        Ok(())
    }

    fn save(&self) -> Result<(), DbError> {
        Ok(()) // 内存存储无需持久化
    }

    fn load(&mut self) -> Result<(), DbError> {
        Ok(()) // 内存存储无需加载
    }
} 