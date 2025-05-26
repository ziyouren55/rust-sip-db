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
    Int(Option<usize>), // 整数类型可选位数
    Varchar(usize),     // 存储varchar的最大长度
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Column {
    pub name: String,
    pub data_type: ColumnType,
    pub nullable: bool,
    pub primary_key: bool, // 新增主键标识
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
    
    #[error("Error: Field '{0}' doesn't have a default value")]
    NullValue(String),

    #[error("Error: Duplicate entry '{0}' for key 'PRIMARY'")]
    PrimaryKeyViolation(String),
}

impl DataType {
    pub fn matches_column_type(&self, column_type: &ColumnType) -> bool {
        match (self, column_type) {
            (DataType::Int(_), ColumnType::Int(_)) => true,
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

    // 检查主键是否重复
    fn check_primary_key_constraint(&self, row: &[DataType]) -> Result<(), TypeError> {
        // 查找主键列的索引
        let primary_key_index = self.columns.iter().position(|col| col.primary_key);
        
        if let Some(pk_index) = primary_key_index {
            // 获取要插入的主键值
            let pk_value = &row[pk_index];
            
            // 跳过NULL值的主键检查（虽然主键通常不允许为NULL）
            if let DataType::Null = pk_value {
                return Ok(());
            }
            
            // 检查是否有重复的主键值
            for existing_row in &self.rows {
                if &existing_row[pk_index] == pk_value {
                    return Err(TypeError::PrimaryKeyViolation(pk_value.to_string()));
                }
            }
        }
        
        Ok(())
    }

    pub fn validate_row(&self, row: &[DataType]) -> Result<(), TypeError> {
        if row.len() != self.columns.len() {
            return Err(TypeError::TypeMismatch {
                expected: ColumnType::Int(None),
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

            // 检查非空约束
            if !column.nullable && matches!(value, DataType::Null) {
                return Err(TypeError::NullValue(column.name.clone()));
            }
            
            // 检查主键不能为NULL
            if column.primary_key && matches!(value, DataType::Null) {
                return Err(TypeError::NullValue(column.name.clone()));
            }
        }
        
        // 检查主键约束
        self.check_primary_key_constraint(row)?;

        Ok(())
    }

    pub fn insert_row(&mut self, row: Vec<DataType>) -> Result<(), TypeError> {
        self.validate_row(&row)?;
        self.rows.push(row);
        Ok(())
    }
} 