use std::fmt;
use thiserror::Error;
use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum DataType {
    Int(i32),
    Varchar(String),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ColumnType {
    Int,
    Varchar(usize), // 存储varchar的最大长度
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Table {
    pub name: String,
    pub columns: Vec<Column>,
    pub rows: Vec<Vec<DataType>>,
}

#[derive(Error, Debug)]
pub enum TypeError {
    #[error("类型不匹配: 期望 {expected:?}, 实际 {actual:?}")]
    TypeMismatch {
        expected: ColumnType,
        actual: DataType,
    },
    
    #[error("字符串长度超出限制: 最大长度 {max_length}, 实际长度 {actual_length}")]
    StringLengthExceeded {
        max_length: usize,
        actual_length: usize,
    },
    
    #[error("非空字段不能为null")]
    NullValue,
}

impl DataType {
    pub fn matches_column_type(&self, column_type: &ColumnType) -> bool {
        match (self, column_type) {
            (DataType::Int(_), ColumnType::Int) => true,
            (DataType::Varchar(s), ColumnType::Varchar(max_len)) => s.len() <= *max_len,
            (DataType::Null, _) => true,
            _ => false,
        }
    }
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Int(n) => write!(f, "{}", n),
            DataType::Varchar(s) => write!(f, "{}", s),
            DataType::Null => write!(f, "NULL"),
        }
    }
}

impl Table {
    pub fn new(name: String, columns: Vec<Column>) -> Self {
        Table {
            name,
            columns,
            rows: Vec::new(),
        }
    }

    pub fn validate_row(&self, row: &[DataType]) -> Result<(), TypeError> {
        if row.len() != self.columns.len() {
            return Err(TypeError::TypeMismatch {
                expected: ColumnType::Int,
                actual: DataType::Null,
            });
        }

        for (i, (value, column)) in row.iter().zip(&self.columns).enumerate() {
            if !value.matches_column_type(&column.data_type) {
                return Err(TypeError::TypeMismatch {
                    expected: column.data_type.clone(),
                    actual: value.clone(),
                });
            }

            if !column.nullable && matches!(value, DataType::Null) {
                return Err(TypeError::NullValue);
            }
        }

        Ok(())
    }

    pub fn insert_row(&mut self, row: Vec<DataType>) -> Result<(), TypeError> {
        self.validate_row(&row)?;
        self.rows.push(row);
        Ok(())
    }
} 