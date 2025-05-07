use std::fs;
use std::path::PathBuf;
use serde_json;
use crate::core::error::DbError;
use crate::core::types::{Table, DataType};
use super::Storage;

pub struct FileStorage {
    path: PathBuf,
    tables: Vec<Table>,
}

impl FileStorage {
    pub fn new(path: PathBuf) -> Self {
        let mut storage = FileStorage {
            path,
            tables: Vec::new(),
        };
        // 尝试加载现有数据
        let _ = storage.load();
        storage
    }
}

impl Storage for FileStorage {
    fn create_table(&mut self, table: Table) -> Result<(), DbError> {
        if self.tables.iter().any(|t| t.name == table.name) {
            return Err(DbError::TableError(format!("表 {} 已存在", table.name)));
        }
        self.tables.push(table);
        self.save()
    }

    fn drop_table(&mut self, table_name: &str) -> Result<(), DbError> {
        if let Some(index) = self.tables.iter().position(|t| t.name == table_name) {
            self.tables.remove(index);
            self.save()?;
            Ok(())
        } else {
            Err(DbError::TableError(format!("表 {} 不存在", table_name)))
        }
    }

    fn get_table(&self, table_name: &str) -> Result<Option<&Table>, DbError> {
        Ok(self.tables.iter().find(|t| t.name == table_name))
    }

    fn get_table_mut(&mut self, table_name: &str) -> Result<Option<&mut Table>, DbError> {
        Ok(self.tables.iter_mut().find(|t| t.name == table_name))
    }

    fn list_tables(&self) -> Result<Vec<String>, DbError> {
        Ok(self.tables.iter().map(|t| t.name.clone()).collect())
    }

    fn insert_row(&mut self, table_name: &str, row: Vec<DataType>) -> Result<(), DbError> {
        let table = self.get_table_mut(table_name)?
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        table.insert_row(row)?;
        self.save()
    }

    fn delete_row(&mut self, table_name: &str, row_index: usize) -> Result<(), DbError> {
        let table = self.get_table_mut(table_name)?
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        if row_index < table.rows.len() {
            table.rows.remove(row_index);
            self.save()?;
            Ok(())
        } else {
            Err(DbError::TableError(format!("行索引 {} 超出范围", row_index)))
        }
    }

    fn update_row(&mut self, table_name: &str, row_index: usize, row: Vec<DataType>) -> Result<(), DbError> {
        let table = self.get_table_mut(table_name)?
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        table.validate_row(&row)?;
        if row_index < table.rows.len() {
            table.rows[row_index] = row;
            self.save()?;
            Ok(())
        } else {
            Err(DbError::TableError(format!("行索引 {} 超出范围", row_index)))
        }
    }

    fn save(&self) -> Result<(), DbError> {
        let json = serde_json::to_string_pretty(&self.tables)
            .map_err(|e| DbError::Serialization(e.to_string()))?;
        fs::write(&self.path, json)
            .map_err(|e| DbError::IoError(e))?;
        Ok(())
    }

    fn load(&mut self) -> Result<(), DbError> {
        if self.path.exists() {
            let content = fs::read_to_string(&self.path)
                .map_err(|e| DbError::IoError(e))?;
            self.tables = serde_json::from_str(&content)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
        }
        Ok(())
    }
} 