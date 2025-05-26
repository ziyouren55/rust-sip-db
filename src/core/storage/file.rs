use std::fs;
use std::path::PathBuf;
use std::collections::HashMap;
use serde_json;
use crate::core::error::DbError;
use crate::core::types::{Table, DataType};
use super::Storage;

pub struct FileStorage {
    base_dir: PathBuf,
    tables: HashMap<String, Table>,
}

impl FileStorage {
    pub fn new(base_dir: PathBuf) -> Self {
        let tables_dir = base_dir.join("tables");
        
        // 确保表目录存在
        if !tables_dir.exists() {
            let _ = fs::create_dir_all(&tables_dir);
        }
        
        let mut storage = FileStorage {
            base_dir,
            tables: HashMap::new(),
        };
        
        // 加载所有表
        let _ = storage.load();
        storage
    }
    
    // 获取表文件路径
    fn get_table_path(&self, table_name: &str) -> PathBuf {
        self.base_dir.join("tables").join(format!("{}.json", table_name))
    }
    
    // 加载单个表
    fn load_table(&mut self, table_name: &str) -> Result<(), DbError> {
        let table_path = self.get_table_path(table_name);
        
        if table_path.exists() {
            let content = fs::read_to_string(&table_path)
                .map_err(|e| DbError::IoError(e))?;
            let table: Table = serde_json::from_str(&content)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            self.tables.insert(table_name.to_string(), table);
        }
        
        Ok(())
    }
    
    // 保存单个表
    fn save_table(&self, table_name: &str) -> Result<(), DbError> {
        if let Some(table) = self.tables.get(table_name) {
            let table_path = self.get_table_path(table_name);
            let json = serde_json::to_string_pretty(table)
                .map_err(|e| DbError::Serialization(e.to_string()))?;
            fs::write(&table_path, json)
                .map_err(|e| DbError::IoError(e))?;
        }
        
        Ok(())
    }
}

impl Storage for FileStorage {
    fn create_table(&mut self, table: Table) -> Result<(), DbError> {
        let table_name = table.name.clone();
        
        if self.tables.contains_key(&table_name) {
            return Err(DbError::TableError(format!("表 {} 已存在", table_name)));
        }
        
        self.tables.insert(table_name.clone(), table);
        self.save_table(&table_name)
    }

    fn drop_table(&mut self, table_name: &str) -> Result<(), DbError> {
        if self.tables.remove(table_name).is_some() {
            // 删除表文件
            let table_path = self.get_table_path(table_name);
            if table_path.exists() {
                fs::remove_file(table_path)
                    .map_err(|e| DbError::IoError(e))?;
            }
            Ok(())
        } else {
            Err(DbError::TableError(format!("表 {} 不存在", table_name)))
        }
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

    fn get_tables(&self) -> Result<Vec<&Table>, DbError> {
        Ok(self.tables.values().collect())
    }

    fn get_table_by_index(&self, index: usize) -> Result<Option<&Table>, DbError> {
        Ok(self.tables.values().nth(index))
    }

    fn insert_row(&mut self, table_name: &str, row: Vec<DataType>) -> Result<(), DbError> {
        let table = self.get_table_mut(table_name)?
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        
        // 直接调用insert_row，保留原始错误类型
        table.insert_row(row)?;
        self.save_table(table_name)
    }

    fn delete_row(&mut self, table_name: &str, row_index: usize) -> Result<(), DbError> {
        let table = self.get_table_mut(table_name)?
            .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;
        if row_index < table.rows.len() {
            table.rows.remove(row_index);
            self.save_table(table_name)?;
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
            self.save_table(table_name)?;
            Ok(())
        } else {
            Err(DbError::TableError(format!("行索引 {} 超出范围", row_index)))
        }
    }

    fn save(&self) -> Result<(), DbError> {
        // 保存所有表
        for table_name in self.tables.keys() {
            self.save_table(table_name)?;
        }
        Ok(())
    }

    fn load(&mut self) -> Result<(), DbError> {
        // 清空现有表
        self.tables.clear();
        
        // 获取tables目录下的所有json文件
        let tables_dir = self.base_dir.join("tables");
        if tables_dir.exists() {
            let entries = fs::read_dir(&tables_dir)
                .map_err(|e| DbError::IoError(e))?;
            
            for entry in entries {
                if let Ok(entry) = entry {
                    let path = entry.path();
                    if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
                        if let Some(file_stem) = path.file_stem() {
                            if let Some(table_name) = file_stem.to_str() {
                                self.load_table(table_name)?;
        }
                        }
                    }
                }
            }
        }
        
        Ok(())
    }
} 