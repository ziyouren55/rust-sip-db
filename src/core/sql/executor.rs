use crate::core::error::DbError;
use crate::core::types::{DataType, Table, TypeError};
use crate::core::storage::Storage;
use super::{SqlStatement, WhereClause, Operator, TableFormatter};

pub struct SqlExecutor<'a> {
    storage: &'a mut dyn Storage,
    has_output: bool,
}

impl<'a> SqlExecutor<'a> {
    pub fn new(storage: &'a mut dyn Storage) -> Self {
        SqlExecutor { 
            storage,
            has_output: false,
        }
    }

    pub fn has_output(&self) -> bool {
        self.has_output
    }

    pub fn execute(&mut self, statement: SqlStatement) -> Result<(), DbError> {
        self.has_output = false;
        
        match statement {
            SqlStatement::CreateTable { name, columns } => {
                let table = Table::new(name, columns);
                self.storage.create_table(table)
            }
            SqlStatement::DropTable { name } => {
                self.storage.drop_table(&name)
            }
            SqlStatement::DropTables { names } => {
                // 依次删除每个表
                for name in names {
                    // 如果某个表不存在，记录错误但继续处理其它表
                    if let Err(err) = self.storage.drop_table(&name) {
                        eprintln!("删除表 {} 时出错: {}", name, err);
                    }
                }
                Ok(())
            }
            SqlStatement::Insert { table, values } => {
                // 获取表结构以检查主键
                let table_struct = self.storage.get_table(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;
                
                // 克隆表结构相关信息，避免借用冲突
                let table_columns = table_struct.columns.clone();
                
                // 检查值的数量是否与表列数匹配
                if values.len() != table_columns.len() {
                    return Err(DbError::SqlError(format!(
                        "值的数量({})与表列数({})不匹配", 
                        values.len(), table_columns.len()
                    )));
                }
                
                // 检查主键和非空约束
                for (i, col) in table_columns.iter().enumerate() {
                    // 检查主键
                    if col.primary_key && matches!(values[i], DataType::Null) {
                        return Err(DbError::TypeError(TypeError::NullValue(col.name.clone())));
                    }
                    
                    // 检查非空约束
                    if !col.nullable && matches!(values[i], DataType::Null) {
                        return Err(DbError::TypeError(TypeError::NullValue(col.name.clone())));
                    }
                }
                
                self.storage.insert_row(&table, values)
            }
            SqlStatement::InsertMultiple { table, rows } => {
                // 获取表结构以检查主键
                let table_struct = self.storage.get_table(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;
                
                // 克隆表结构相关信息，避免借用冲突
                let table_columns = table_struct.columns.clone();
                
                // 依次插入每一行数据
                for values in rows {
                    // 检查值的数量是否与表列数匹配
                    if values.len() != table_columns.len() {
                        return Err(DbError::SqlError(format!(
                            "值的数量({})与表列数({})不匹配", 
                            values.len(), table_columns.len()
                        )));
                    }
                    
                    // 检查主键和非空约束
                    for (i, col) in table_columns.iter().enumerate() {
                        // 检查主键
                        if col.primary_key && matches!(values[i], DataType::Null) {
                            return Err(DbError::TypeError(TypeError::NullValue(col.name.clone())));
                        }
                        
                        // 检查非空约束
                        if !col.nullable && matches!(values[i], DataType::Null) {
                            return Err(DbError::TypeError(TypeError::NullValue(col.name.clone())));
                        }
                    }
                    
                    self.storage.insert_row(&table, values)?;
                }
                Ok(())
            }
            SqlStatement::InsertWithColumns { table, columns, rows } => {
                // 获取表结构
                let table_struct = self.storage.get_table(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;
                
                // 克隆表结构相关信息，避免借用冲突
                let table_columns = table_struct.columns.clone();
                
                // 检查列名是否存在于表中
                for col in &columns {
                    if !table_columns.iter().any(|c| &c.name == col) {
                        return Err(DbError::SqlError(format!("列 {} 在表 {} 中不存在", col, table)));
                    }
                }
                
                // 处理每一行数据
                for row_values in rows {
                    // 检查值的数量是否与列名数量匹配
                    if row_values.len() != columns.len() {
                        return Err(DbError::SqlError(format!(
                            "值的数量({})与列名数量({})不匹配", 
                            row_values.len(), columns.len()
                        )));
                    }
                    
                    // 创建完整的行数据（按表的列顺序）
                    let mut full_row = vec![DataType::Null; table_columns.len()];
                    
                    // 填充指定的列
                    for (i, col) in columns.iter().enumerate() {
                        if let Some(col_index) = table_columns.iter().position(|c| &c.name == col) {
                            full_row[col_index] = row_values[i].clone();
                        }
                    }
                    
                    // 检查约束
                    for (i, col) in table_columns.iter().enumerate() {
                        // 检查非空约束
                        if !col.nullable && matches!(full_row[i], DataType::Null) {
                            return Err(DbError::TypeError(TypeError::NullValue(col.name.clone())));
                        }
                        
                        // 检查主键
                        if col.primary_key && matches!(full_row[i], DataType::Null) {
                            return Err(DbError::TypeError(TypeError::NullValue(col.name.clone())));
                        }
                    }
                    
                    // 插入行
                    self.storage.insert_row(&table, full_row)?;
                }
                
                Ok(())
            }
            SqlStatement::SelectExpression { expressions, original_sql } => {
                // 计算每个表达式的值
                let mut results = Vec::new();
                let mut headers = Vec::new();
                
                // 从原始 SQL 中提取表达式部分
                let select_expressions = original_sql.trim_start()
                    .strip_prefix("select")
                    .or_else(|| original_sql.trim_start().strip_prefix("SELECT"))
                    .unwrap_or(&original_sql)
                    .trim()
                    .trim_end_matches(';')
                    .trim();
                
                // 按逗号分割表达式
                let expr_parts: Vec<&str> = select_expressions.split(',').collect();
                
                for (i, expr) in expressions.iter().enumerate() {
                    // 计算表达式
                    let result = self.evaluate_expression(expr, None, "")?;
                    
                    // 使用原始 SQL 中的表达式作为表头
                    let header = if i < expr_parts.len() {
                        expr_parts[i].trim().to_string()
                    } else {
                        // 如果无法找到对应的原始表达式，使用生成的字符串
                        self.expression_to_string(expr)
                    };
                    
                    results.push(result.to_string());
                    headers.push(header);
                }
                
                // 将结果格式化为表格
                let formatted_table = TableFormatter::format_table(&headers, &[results]);
                print!("{}", formatted_table);
                
                self.has_output = true;
                
                Ok(())
            }
            SqlStatement::SelectWithExpressions { expressions, table, where_clause, order_by, original_sql } => {
                let table_data = self.storage.get_table(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;
                
                // 从原始 SQL 中提取 SELECT 部分
                let select_part = original_sql.trim_start()
                    .strip_prefix("select")
                    .or_else(|| original_sql.trim_start().strip_prefix("SELECT"))
                    .unwrap_or(&original_sql)
                    .trim();
                
                // 提取 FROM 之前的部分
                let expr_part = if let Some(from_pos) = select_part.to_lowercase().find("from") {
                    select_part[..from_pos].trim()
                } else {
                    select_part.trim()
                };
                
                // 按逗号分割表达式
                let expr_parts: Vec<&str> = expr_part.split(',').collect();
                
                // 准备表头 - 从原始 SQL 表达式生成
                let mut headers = Vec::new();
                for (i, expr) in expressions.iter().enumerate() {
                    if i < expr_parts.len() {
                        headers.push(expr_parts[i].trim().to_string());
                    } else {
                        // 如果无法找到对应的原始表达式，使用生成的字符串
                        headers.push(self.expression_to_string(expr));
                    }
                }
                
                // 收集满足条件的行数据
                let mut selected_rows: Vec<Vec<String>> = Vec::new();
                for row in &table_data.rows {
                    if where_clause.is_none() || evaluate_where_clause(row, where_clause.as_ref().unwrap(), &table_data.columns)? {
                        // 计算每个表达式的值
                        let mut row_values = Vec::new();
                        for expr in &expressions {
                            // 计算表达式的值
                            let result = self.evaluate_expression(expr, Some(row), &table)?;
                            row_values.push(result.to_string());
                        }
                        selected_rows.push(row_values);
                    }
                }
                
                // 如果有ORDER BY子句，对结果进行排序
                if let Some(order_by) = order_by {
                    self.apply_order_by(&mut selected_rows, &headers, &order_by)?;
                }
                
                // 使用TableFormatter格式化并输出结果
                if !selected_rows.is_empty() {
                    let formatted_table = TableFormatter::format_table(&headers, &selected_rows);
                    print!("{}", formatted_table);
                    self.has_output = true;
                } else {
                    // 对于空结果集，不输出任何信息，改由外部统一处理
                }
                Ok(())
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
            SqlStatement::Select { columns, table, where_clause, order_by } => {
                let table_data = self.storage.get_table(&table)?
                    .ok_or_else(|| DbError::TableError(format!("表 {} 不存在", table)))?;

                // 处理 SELECT * 的情况
                let is_select_all = columns.len() == 1 && columns[0] == "*";
                let display_columns = if is_select_all {
                    // 获取表中所有列名
                    table_data.columns.iter().map(|c| c.name.clone()).collect()
                } else {
                    columns.clone()
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

                // 如果有ORDER BY子句，对结果进行排序
                if let Some(order_by) = order_by {
                    self.apply_order_by(&mut selected_rows, &display_columns, &order_by)?;
                }

                // 使用TableFormatter格式化并输出结果
                if !selected_rows.is_empty() {
                    let formatted_table = TableFormatter::format_table(&display_columns, &selected_rows);
                    print!("{}", formatted_table);
                    self.has_output = true;
                } else {
                    // 对于空结果集，不输出任何信息，改由外部统一处理
                }
                Ok(())
            }
        }
    }

    // 评估表达式的值
    pub fn evaluate_expression(&self, expr: &super::Expression, row: Option<&[DataType]>, current_table: &str) -> Result<DataType, DbError> {
        match expr {
            super::Expression::Literal(value) => Ok(value.clone()),
            super::Expression::Column(name) => {
                if let Some(row_data) = row {
                    // 从表数据中获取列信息
                    if name == "*" {
                        return Err(DbError::SqlError("不能直接使用 * 作为表达式".to_string()));
                    }
                    
                    // 获取当前表
                    let table_name = if name.contains('.') {
                        name.split('.').next().unwrap_or("")
                    } else {
                        // 使用当前查询的表名
                        current_table
                    };
                    
                    // 获取列名
                    let column_name = if name.contains('.') {
                        name.split('.').nth(1).unwrap_or(name)
                    } else {
                        name
                    };
                    
                    // 从存储中获取表定义
                    if let Ok(Some(table)) = self.storage.get_table(table_name) {
                        if let Some(col_index) = table.columns.iter().position(|col| &col.name == column_name) {
                            if col_index < row_data.len() {
                                return Ok(row_data[col_index].clone());
                            }
                        }
                    }
                    
                    // 如果没有找到，尝试在所有表中查找
                    if table_name.is_empty() || table_name == current_table {
                        // 从所有表中查找此列名
                        for (i, _) in self.storage.get_tables()?.iter().enumerate() {
                            if let Ok(Some(table)) = self.storage.get_table_by_index(i) {
                                if &table.name != current_table { // 跳过当前表，因为已经查找过
                                    if let Some(col_index) = table.columns.iter().position(|col| &col.name == name) {
                                        if col_index < row_data.len() {
                                            return Ok(row_data[col_index].clone());
                                        }
                                    }
                                }
                            }
                        }
                    }
                    
                    Err(DbError::SqlError(format!("列 {} 未找到", name)))
                } else {
                    // 没有行上下文，无法获取列值
                    Err(DbError::SqlError("无法获取列值，因为没有行上下文".to_string()))
                }
            },
            super::Expression::Binary { left, operator, right } => {
                let left_value = self.evaluate_expression(left, row, current_table)?;
                let right_value = self.evaluate_expression(right, row, current_table)?;
                
                match (left_value, right_value) {
                    (DataType::Int(a), DataType::Int(b)) => {
                        let result = match operator {
                            super::ArithmeticOperator::Add => a + b,
                            super::ArithmeticOperator::Subtract => a - b,
                            super::ArithmeticOperator::Multiply => a * b,
                            super::ArithmeticOperator::Divide => {
                                if b == 0 {
                                    return Err(DbError::SqlError("除数不能为零".to_string()));
                                }
                                a / b
                            },
                        };
                        Ok(DataType::Int(result))
                    },
                    (DataType::Float(a), DataType::Float(b)) => {
                        let result = match operator {
                            super::ArithmeticOperator::Add => a + b,
                            super::ArithmeticOperator::Subtract => a - b,
                            super::ArithmeticOperator::Multiply => a * b,
                            super::ArithmeticOperator::Divide => {
                                if b == 0.0 {
                                    return Err(DbError::SqlError("除数不能为零".to_string()));
                                }
                                a / b
                            },
                        };
                        Ok(DataType::Float(result))
                    },
                    (DataType::Int(a), DataType::Float(b)) => {
                        let a_float = a as f64;
                        let result = match operator {
                            super::ArithmeticOperator::Add => a_float + b,
                            super::ArithmeticOperator::Subtract => a_float - b,
                            super::ArithmeticOperator::Multiply => a_float * b,
                            super::ArithmeticOperator::Divide => {
                                if b == 0.0 {
                                    return Err(DbError::SqlError("除数不能为零".to_string()));
                                }
                                a_float / b
                            },
                        };
                        Ok(DataType::Float(result))
                    },
                    (DataType::Float(a), DataType::Int(b)) => {
                        let b_float = b as f64;
                        let result = match operator {
                            super::ArithmeticOperator::Add => a + b_float,
                            super::ArithmeticOperator::Subtract => a - b_float,
                            super::ArithmeticOperator::Multiply => a * b_float,
                            super::ArithmeticOperator::Divide => {
                                if b == 0 {
                                    return Err(DbError::SqlError("除数不能为零".to_string()));
                                }
                                a / b_float
                            },
                        };
                        Ok(DataType::Float(result))
                    },
                    // 可以添加更多类型组合的处理
                    _ => Err(DbError::SqlError("不支持的操作数类型".to_string())),
                }
            },
        }
    }
    
    // 将表达式转换为字符串表示
    fn expression_to_string(&self, expr: &super::Expression) -> String {
        match expr {
            super::Expression::Literal(value) => value.to_string(),
            super::Expression::Column(name) => name.clone(),
            super::Expression::Binary { left, operator, right } => {
                let left_str = self.expression_to_string(left);
                let right_str = self.expression_to_string(right);
                let op_str = match operator {
                    super::ArithmeticOperator::Add => "+",
                    super::ArithmeticOperator::Subtract => "-",
                    super::ArithmeticOperator::Multiply => "*",
                    super::ArithmeticOperator::Divide => "/",
                };
                format!("{}{}{}", left_str, op_str, right_str)
            },
        }
    }

    // 应用ORDER BY排序
    fn apply_order_by(&self, rows: &mut Vec<Vec<String>>, headers: &[String], order_by: &super::OrderBy) -> Result<(), DbError> {
        // 查找排序列的索引
        let sort_col_index = headers.iter().position(|col| col == &order_by.column)
            .ok_or_else(|| DbError::SqlError(format!("ORDER BY列 {} 不存在于结果集中", order_by.column)))?;
        
        // 排序
        rows.sort_by(|a, b| {
            let a_val = &a[sort_col_index];
            let b_val = &b[sort_col_index];
            
            // 首先尝试将值解析为数字并比较
            match (a_val.parse::<i64>(), b_val.parse::<i64>()) {
                (Ok(a_num), Ok(b_num)) => {
                    // 数值比较
                    match order_by.direction {
                        super::SortDirection::Asc => a_num.cmp(&b_num),
                        super::SortDirection::Desc => b_num.cmp(&a_num),
                    }
                },
                _ => {
                    // 字符串比较
                    match order_by.direction {
                        super::SortDirection::Asc => a_val.cmp(b_val),
                        super::SortDirection::Desc => b_val.cmp(a_val),
                    }
                }
            }
        });
        
        Ok(())
    }
}

fn evaluate_where_clause(row: &[DataType], where_clause: &WhereClause, columns: &[crate::core::types::Column]) -> Result<bool, DbError> {
    match where_clause {
        WhereClause::Simple { column, operator, value } => {
            let column_index = columns.iter()
                .position(|col| col.name == *column)
                .ok_or_else(|| DbError::SqlError(format!("列 {} 不存在", column)))?;

            let row_value = &row[column_index];
            let compare_value = value;

            let result = match operator {
                Operator::Eq => row_value == compare_value,
                Operator::Ne => row_value != compare_value,
                Operator::Gt => match (row_value, compare_value) {
                    (DataType::Int(a), DataType::Int(b)) => a > b,
                    (DataType::Float(a), DataType::Float(b)) => a > b,
                    (DataType::Float(a), DataType::Int(b)) => a > &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) > b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a > b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::Lt => match (row_value, compare_value) {
                    (DataType::Int(a), DataType::Int(b)) => a < b,
                    (DataType::Float(a), DataType::Float(b)) => a < b,
                    (DataType::Float(a), DataType::Int(b)) => a < &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) < b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a < b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::Ge => match (row_value, compare_value) {
                    (DataType::Int(a), DataType::Int(b)) => a >= b,
                    (DataType::Float(a), DataType::Float(b)) => a >= b,
                    (DataType::Float(a), DataType::Int(b)) => a >= &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) >= b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a >= b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::Le => match (row_value, compare_value) {
                    (DataType::Int(a), DataType::Int(b)) => a <= b,
                    (DataType::Float(a), DataType::Float(b)) => a <= b,
                    (DataType::Float(a), DataType::Int(b)) => a <= &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) <= b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a <= b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::IsNull => matches!(row_value, DataType::Null),
                Operator::IsNotNull => !matches!(row_value, DataType::Null),
            };

            Ok(result)
        },
        WhereClause::Expression { left, operator, right } => {
            // 使用不需要存储引用的函数评估表达式
            let left_value = evaluate_expression_without_storage(left, row, columns)?;
            let right_value = evaluate_expression_without_storage(right, row, columns)?;
            
            // 比较两个表达式的结果
            let result = match operator {
                Operator::Eq => left_value == right_value,
                Operator::Ne => left_value != right_value,
                Operator::Gt => match (&left_value, &right_value) {
                    (DataType::Int(a), DataType::Int(b)) => a > b,
                    (DataType::Float(a), DataType::Float(b)) => a > b,
                    (DataType::Float(a), DataType::Int(b)) => a > &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) > b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a > b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::Lt => match (&left_value, &right_value) {
                    (DataType::Int(a), DataType::Int(b)) => a < b,
                    (DataType::Float(a), DataType::Float(b)) => a < b,
                    (DataType::Float(a), DataType::Int(b)) => a < &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) < b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a < b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::Ge => match (&left_value, &right_value) {
                    (DataType::Int(a), DataType::Int(b)) => a >= b,
                    (DataType::Float(a), DataType::Float(b)) => a >= b,
                    (DataType::Float(a), DataType::Int(b)) => a >= &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) >= b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a >= b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::Le => match (&left_value, &right_value) {
                    (DataType::Int(a), DataType::Int(b)) => a <= b,
                    (DataType::Float(a), DataType::Float(b)) => a <= b,
                    (DataType::Float(a), DataType::Int(b)) => a <= &(*b as f64),
                    (DataType::Int(a), DataType::Float(b)) => &(*a as f64) <= b,
                    (DataType::Varchar(a), DataType::Varchar(b)) => a <= b,
                    _ => return Err(DbError::SqlError("类型不匹配".to_string())),
                },
                Operator::IsNull => matches!(left_value, DataType::Null),
                Operator::IsNotNull => !matches!(left_value, DataType::Null),
            };
            
            Ok(result)
        },
        WhereClause::And { left, right } => {
            // 对于 AND，两边都需要为真
            let left_result = evaluate_where_clause(row, left, columns)?;
            
            // 短路求值：如果左边为假，直接返回假
            if !left_result {
                return Ok(false);
            }
            
            let right_result = evaluate_where_clause(row, right, columns)?;
            Ok(left_result && right_result)
        },
        WhereClause::Or { left, right } => {
            // 对于 OR，只需一边为真
            let left_result = evaluate_where_clause(row, left, columns)?;
            
            // 短路求值：如果左边为真，直接返回真
            if left_result {
                return Ok(true);
            }
            
            let right_result = evaluate_where_clause(row, right, columns)?;
            Ok(left_result || right_result)
        },
    }
}

// 不使用存储引用的表达式求值函数，用于WHERE子句评估
pub fn evaluate_expression_without_storage(expr: &super::Expression, row: &[DataType], columns: &[crate::core::types::Column]) -> Result<DataType, DbError> {
    match expr {
        super::Expression::Literal(value) => Ok(value.clone()),
        super::Expression::Column(name) => {
            if name == "*" {
                return Err(DbError::SqlError("不能直接使用 * 作为表达式".to_string()));
            }
            
            // 获取列名（不考虑表名前缀，因为WHERE子句通常只涉及当前表）
            let column_name = if name.contains('.') {
                name.split('.').nth(1).unwrap_or(name)
            } else {
                name
            };
            
            // 获取列索引
            let col_index = columns.iter()
                .position(|col| &col.name == column_name)
                .ok_or_else(|| DbError::SqlError(format!("列 {} 未找到", name)))?;
            
            if col_index < row.len() {
                Ok(row[col_index].clone())
            } else {
                Err(DbError::SqlError(format!("索引超出范围: {}", col_index)))
            }
        },
        super::Expression::Binary { left, operator, right } => {
            let left_value = evaluate_expression_without_storage(left, row, columns)?;
            let right_value = evaluate_expression_without_storage(right, row, columns)?;
            
            match (left_value, right_value) {
                (DataType::Int(a), DataType::Int(b)) => {
                    let result = match operator {
                        super::ArithmeticOperator::Add => a + b,
                        super::ArithmeticOperator::Subtract => a - b,
                        super::ArithmeticOperator::Multiply => a * b,
                        super::ArithmeticOperator::Divide => {
                            if b == 0 {
                                return Err(DbError::SqlError("除数不能为零".to_string()));
                            }
                            a / b
                        },
                    };
                    Ok(DataType::Int(result))
                },
                (DataType::Float(a), DataType::Float(b)) => {
                    let result = match operator {
                        super::ArithmeticOperator::Add => a + b,
                        super::ArithmeticOperator::Subtract => a - b,
                        super::ArithmeticOperator::Multiply => a * b,
                        super::ArithmeticOperator::Divide => {
                            if b == 0.0 {
                                return Err(DbError::SqlError("除数不能为零".to_string()));
                            }
                            a / b
                        },
                    };
                    Ok(DataType::Float(result))
                },
                (DataType::Int(a), DataType::Float(b)) => {
                    let a_float = a as f64;
                    let result = match operator {
                        super::ArithmeticOperator::Add => a_float + b,
                        super::ArithmeticOperator::Subtract => a_float - b,
                        super::ArithmeticOperator::Multiply => a_float * b,
                        super::ArithmeticOperator::Divide => {
                            if b == 0.0 {
                                return Err(DbError::SqlError("除数不能为零".to_string()));
                            }
                            a_float / b
                        },
                    };
                    Ok(DataType::Float(result))
                },
                (DataType::Float(a), DataType::Int(b)) => {
                    let b_float = b as f64;
                    let result = match operator {
                        super::ArithmeticOperator::Add => a + b_float,
                        super::ArithmeticOperator::Subtract => a - b_float,
                        super::ArithmeticOperator::Multiply => a * b_float,
                        super::ArithmeticOperator::Divide => {
                            if b == 0 {
                                return Err(DbError::SqlError("除数不能为零".to_string()));
                            }
                            a / b_float
                        },
                    };
                    Ok(DataType::Float(result))
                },
                _ => Err(DbError::SqlError("不支持的操作数类型".to_string())),
            }
        },
    }
} 