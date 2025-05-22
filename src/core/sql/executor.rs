use crate::core::error::DbError;
use crate::core::types::{DataType, Table};
use crate::core::storage::Storage;
use super::{SqlStatement, WhereClause, Operator, TableFormatter};

pub struct SqlExecutor<'a> {
    storage: &'a mut dyn Storage,
}

impl<'a> SqlExecutor<'a> {
    pub fn new(storage: &'a mut dyn Storage) -> Self {
        SqlExecutor { storage }
    }

    pub fn execute(&mut self, statement: SqlStatement) -> Result<(), DbError> {
        match statement {
            SqlStatement::CreateTable { name, columns } => {
                let table = Table::new(name, columns);
                self.storage.create_table(table)
            }
            SqlStatement::DropTable { name } => {
                self.storage.drop_table(&name)
            }
            SqlStatement::Insert { table, values } => {
                self.storage.insert_row(&table, values)
            }
            SqlStatement::Update { table, set, where_clause } => {
                let table_data = self.storage.get_table_mut(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;
                
                let columns = table_data.columns.clone();
                let mut rows_to_update = Vec::new();
                
                // 找出需要更新的行
                for (i, row) in table_data.rows.iter().enumerate() {
                    if where_clause.is_none() || evaluate_where_clause(row, where_clause.as_ref().unwrap(), &columns)? {
                        rows_to_update.push(i);
                    }
                }

                // 更新行
                for row_index in rows_to_update {
                    for (column_name, value) in &set {
                        if let Some(col_index) = table_data.columns.iter().position(|col| &col.name == column_name) {
                            table_data.rows[row_index][col_index] = value.clone();
                        }
                    }
                }
                Ok(())
            }
            SqlStatement::Delete { table, where_clause } => {
                let table_data = self.storage.get_table_mut(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;

                if where_clause.is_none() {
                    table_data.rows.clear();
                    return Ok(());
                }

                let columns = table_data.columns.clone();
                let where_clause = where_clause.unwrap();
                let mut i = 0;
                while i < table_data.rows.len() {
                    if evaluate_where_clause(&table_data.rows[i], &where_clause, &columns)? {
                        table_data.rows.remove(i);
                    } else {
                        i += 1;
                    }
                }
                Ok(())
            }
            SqlStatement::Select { columns, table, where_clause } => {
                let table_data = self.storage.get_table(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;

                // 处理 SELECT * 的情况
                let is_select_all = columns.len() == 1 && columns[0] == "*";
                let display_columns = if is_select_all {
                    // 获取表中所有列名
                    table_data.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    columns
                };

                // 收集满足条件的行数据
                let mut selected_rows: Vec<Vec<String>> = Vec::new();
                for row in &table_data.rows {
                    if where_clause.is_none() || evaluate_where_clause(row, where_clause.as_ref().unwrap(), &table_data.columns)? {
                        let values: Vec<String> = if is_select_all {
                            // 如果是 SELECT *，获取所有列的值
                            row.iter().map(|val| val.to_string()).collect()
                        } else {
                            // 否则只获取指定列的值
                            display_columns.iter().map(|col| {
                                if let Some(index) = table_data.columns.iter().position(|c| &c.name == col) {
                                    row[index].to_string()
                                } else {
                                    "NULL".to_string()
                                }
                            }).collect()
                        };
                        selected_rows.push(values);
                    }
                }

                // 使用TableFormatter格式化并输出结果
                if !selected_rows.is_empty() {
                    let formatted_table = TableFormatter::format_table(&display_columns, &selected_rows);
                    print!("{}", formatted_table);
                } else {
                    println!("查询结果为空");
                }
                Ok(())
            }
        }
    }
}

fn evaluate_where_clause(row: &[DataType], where_clause: &WhereClause, columns: &[crate::core::types::Column]) -> Result<bool, DbError> {
    let column_index = columns.iter()
        .position(|col| col.name == where_clause.column)
        .ok_or_else(|| DbError::SqlError(format!("列 {} 不存在", where_clause.column)))?;

    let value = &row[column_index];
    let compare_value = &where_clause.value;

    let result = match where_clause.operator {
        Operator::Eq => value == compare_value,
        Operator::Ne => value != compare_value,
        Operator::Gt => match (value, compare_value) {
            (DataType::Int(a), DataType::Int(b)) => a > b,
            (DataType::Varchar(a), DataType::Varchar(b)) => a > b,
            _ => return Err(DbError::SqlError("类型不匹配".to_string())),
        },
        Operator::Lt => match (value, compare_value) {
            (DataType::Int(a), DataType::Int(b)) => a < b,
            (DataType::Varchar(a), DataType::Varchar(b)) => a < b,
            _ => return Err(DbError::SqlError("类型不匹配".to_string())),
        },
        Operator::Ge => match (value, compare_value) {
            (DataType::Int(a), DataType::Int(b)) => a >= b,
            (DataType::Varchar(a), DataType::Varchar(b)) => a >= b,
            _ => return Err(DbError::SqlError("类型不匹配".to_string())),
        },
        Operator::Le => match (value, compare_value) {
            (DataType::Int(a), DataType::Int(b)) => a <= b,
            (DataType::Varchar(a), DataType::Varchar(b)) => a <= b,
            _ => return Err(DbError::SqlError("类型不匹配".to_string())),
        },
    };

    Ok(result)
} 