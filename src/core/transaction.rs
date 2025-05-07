use std::collections::HashMap;
use crate::core::error::DbError;
use crate::core::storage::Storage;
use crate::core::types::{Table, DataType};

#[derive(PartialEq)]
pub enum TransactionState {
    Active,
    Committed,
    RolledBack,
}

pub struct Transaction<'a> {
    storage: &'a mut dyn Storage,
    state: TransactionState,
    table_changes: HashMap<String, Vec<TableChange>>,
}

#[derive(Debug)]
enum TableChange {
    Insert(Vec<DataType>),
    Update { row_index: usize, row: Vec<DataType> },
    Delete(usize),
}

impl<'a> Transaction<'a> {
    pub fn new(storage: &'a mut dyn Storage) -> Self {
        Transaction {
            storage,
            state: TransactionState::Active,
            table_changes: HashMap::new(),
        }
    }

    pub fn create_table(&mut self, name: String, columns: Vec<crate::core::types::Column>) -> Result<(), DbError> {
        if self.state != TransactionState::Active {
            return Err(DbError::TransactionError("Transaction is not active".to_string()));
        }
        let table = Table::new(name.clone(), columns);
        self.storage.create_table(table)?;
        self.table_changes.insert(name, Vec::new());
        Ok(())
    }

    pub fn drop_table(&mut self, table_name: &str) -> Result<(), DbError> {
        if self.state != TransactionState::Active {
            return Err(DbError::TransactionError("Transaction is not active".to_string()));
        }
        self.storage.drop_table(table_name)?;
        self.table_changes.remove(table_name);
        Ok(())
    }

    pub fn insert_row(&mut self, table_name: &str, row: Vec<DataType>) -> Result<(), DbError> {
        let changes = self.table_changes
            .entry(table_name.to_string())
            .or_insert_with(Vec::new);
        changes.push(TableChange::Insert(row));
        Ok(())
    }

    pub fn update_row(&mut self, table_name: &str, row_index: usize, row: Vec<DataType>) -> Result<(), DbError> {
        let changes = self.table_changes
            .entry(table_name.to_string())
            .or_insert_with(Vec::new);
        changes.push(TableChange::Update { row_index, row });
        Ok(())
    }

    pub fn delete_row(&mut self, table_name: &str, row_index: usize) -> Result<(), DbError> {
        let changes = self.table_changes
            .entry(table_name.to_string())
            .or_insert_with(Vec::new);
        changes.push(TableChange::Delete(row_index));
        Ok(())
    }

    pub fn commit(self) -> Result<(), DbError> {
        // 将所有更改应用到存储
        for (table_name, changes) in self.table_changes {
            let table = self.storage.get_table_mut(&table_name)?
                .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table_name)))?;

            for change in changes {
                match change {
                    TableChange::Insert(row) => {
                        table.insert_row(row)?;
                    }
                    TableChange::Update { row_index, row } => {
                        if row_index < table.rows.len() {
                            table.rows[row_index] = row;
                        }
                    }
                    TableChange::Delete(row_index) => {
                        if row_index < table.rows.len() {
                            table.rows.remove(row_index);
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn rollback(self) -> Result<(), DbError> {
        // 不需要做任何事情，因为更改还没有应用到存储
        Ok(())
    }
} 