use crate::core::error::DbError;
use crate::core::types::{Column, ColumnType, DataType};
use super::lexer::{Token, Lexer};
use super::SqlStatement;

pub struct Parser {
    tokens: Vec<Token>,
    position: usize,
}

impl Parser {
    pub fn new() -> Self {
        Parser {
            tokens: Vec::new(),
            position: 0,
        }
    }

    pub fn parse(&mut self, tokens: Vec<Token>, original_sql: &str) -> Result<SqlStatement, DbError> {
        // 过滤掉所有注释Token
        self.tokens = tokens.into_iter()
            .filter(|token| !matches!(token, Token::Comment(_) | Token::MultiLineComment(_)))
            .collect();
        self.position = 0;
        
        // 如果过滤后没有Token，返回空语句错误
        if self.tokens.is_empty() {
            return Err(DbError::SqlError("空语句或仅包含注释".to_string()));
        }
        
        // 解析语句，并传递原始SQL
        self.parse_statement(original_sql)
    }

    fn parse_statement(&mut self, original_sql: &str) -> Result<SqlStatement, DbError> {
        let current_token = self.peek().cloned();
        
        match current_token {
            Some(Token::Create) => self.parse_create_table(),
            Some(Token::Drop) => self.parse_drop_table(),
            Some(Token::Insert) => self.parse_insert(),
            Some(Token::Update) => self.parse_update(),
            Some(Token::Delete) => self.parse_delete(),
            Some(Token::Select) => {
                // 检查下一个非空位置的 token
                self.next(); // 消费 SELECT
                
                // 保存当前位置以便回溯
                let current_position = self.position;
                
                // 尝试解析表达式查询
                if let Ok(expr_stmt) = self.parse_expression_select(original_sql) {
                    return Ok(expr_stmt);
                }
                
                // 如果不是表达式查询，恢复位置并解析普通查询
                self.position = current_position;
                self.parse_normal_select(original_sql)
            },
            Some(token) => Err(DbError::SqlError(format!("意外的语句开始: {:?}", token))),
            None => Err(DbError::SqlError("空语句".to_string())),
        }
    }

    fn parse_create_table(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Create)?;
        self.expect(Token::Table)?;
        
        let name = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        self.expect(Token::LParen)?;
        let mut columns = Vec::new();
        
        loop {
            let column_name = match self.next() {
                Some(Token::Identifier(name)) => name,
                _ => return Err(DbError::SqlError("期望列名".to_string())),
            };

            let data_type = self.parse_column_type()?;
            let nullable = self.parse_nullable()?;
            let primary_key = self.parse_primary_key()?;
            
            columns.push(Column {
                name: column_name,
                data_type,
                nullable,
                primary_key,
            });

            match self.peek() {
                Some(Token::Comma) => {
                    self.next();
                    continue;
                }
                Some(Token::RParen) => {
                    self.next();
                    break;
                }
                _ => return Err(DbError::SqlError("期望逗号或右括号".to_string())),
            }
        }

        // 确保只有一个主键
        let primary_key_count = columns.iter().filter(|c| c.primary_key).count();
        if primary_key_count > 1 {
            return Err(DbError::SqlError("表中只能有一个主键".to_string()));
        }

        Ok(SqlStatement::CreateTable { name, columns })
    }

    fn parse_column_type(&mut self) -> Result<ColumnType, DbError> {
        match self.next() {
            Some(Token::Identifier(type_name)) => {
                match type_name.to_uppercase().as_str() {
                    "INT" => {
                        // 检查是否有位数标注
                        if let Some(Token::LParen) = self.peek() {
                            self.next(); // 消费左括号
                            let bits = match self.next() {
                                Some(Token::Number(n)) => n as usize,
                                _ => return Err(DbError::SqlError("期望整数位数".to_string())),
                            };
                            self.expect(Token::RParen)?;
                            Ok(ColumnType::Int(Some(bits)))
                        } else {
                            Ok(ColumnType::Int(None))
                        }
                    },
                    "VARCHAR" => {
                        self.expect(Token::LParen)?;
                        let length = match self.next() {
                            Some(Token::Number(n)) => n as usize,
                            _ => return Err(DbError::SqlError("期望VARCHAR长度".to_string())),
                        };
                        self.expect(Token::RParen)?;
                        Ok(ColumnType::Varchar(length))
                    }
                    _ => Err(DbError::SqlError(format!("未知数据类型: {}", type_name))),
                }
            }
            _ => Err(DbError::SqlError("期望数据类型".to_string())),
        }
    }

    fn parse_nullable(&mut self) -> Result<bool, DbError> {
        // 检查是否有NOT NULL
        if let Some(Token::Identifier(ident)) = self.peek() {
            if ident.to_uppercase() == "NOT" {
                self.next(); // 消费NOT
                
                // 期望后面是NULL
                match self.next() {
                    Some(Token::Null) => return Ok(false),
                    Some(Token::Identifier(ident)) if ident.to_uppercase() == "NULL" => return Ok(false),
                    _ => return Err(DbError::SqlError("期望NULL关键字".to_string())),
                }
            }
        }
        
        // 检查是否有明确的NULL
        if let Some(Token::Null) = self.peek() {
            self.next(); // 消费NULL
            return Ok(true);
        } else if let Some(Token::Identifier(ident)) = self.peek() {
            if ident.to_uppercase() == "NULL" {
                self.next(); // 消费NULL
                return Ok(true);
            }
        }
        
        // 如果没有明确指定，默认为可空
        Ok(true)
    }

    fn parse_primary_key(&mut self) -> Result<bool, DbError> {
        if let Some(Token::Primary) = self.peek() {
            self.next(); // 消费PRIMARY
            self.expect(Token::Key)?; // 消费KEY
            Ok(true)
        } else {
            Ok(false)
        }
    }

    fn parse_drop_table(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Drop)?;
        self.expect(Token::Table)?;
        
        // 解析第一个表名
        let name = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        // 检查是否有更多表名（以逗号分隔）
        if let Some(&Token::Comma) = self.peek() {
            // 有多个表名，转为DropTables处理
            let mut names = vec![name];
            
            // 解析剩余表名
            while let Some(&Token::Comma) = self.peek() {
                self.next(); // 消费逗号
                
                match self.next() {
                    Some(Token::Identifier(name)) => names.push(name),
                    _ => return Err(DbError::SqlError("期望表名".to_string())),
                }
            }
            
            Ok(SqlStatement::DropTables { names })
        } else {
            // 只有一个表名
            Ok(SqlStatement::DropTable { name })
        }
    }

    fn parse_insert(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        
        let table = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        // 检查是否有列名列表
        let columns = if let Some(&Token::LParen) = self.peek() {
            self.next(); // 消费左括号
            
            let mut columns = Vec::new();
            loop {
                match self.next() {
                    Some(Token::Identifier(col_name)) => columns.push(col_name),
                    _ => return Err(DbError::SqlError("期望列名".to_string())),
                }
                
                match self.peek() {
                    Some(&Token::Comma) => {
                        self.next(); // 消费逗号
                        continue;
                    }
                    Some(&Token::RParen) => {
                        self.next(); // 消费右括号
                        break;
                    }
                    _ => return Err(DbError::SqlError("期望逗号或右括号".to_string())),
                }
            }
            
            Some(columns)
        } else {
            None
        };

        self.expect(Token::Values)?;
        
        // 检查是否有多行值
        let mut rows = Vec::new();
        let mut first_row = Vec::new();
        
        // 处理第一行
        self.expect(Token::LParen)?;
        loop {
            let value = self.parse_value()?;
            first_row.push(value);

            let next_token = self.peek().cloned();
            match next_token {
                Some(Token::Comma) => {
                    self.next();
                    continue;
                }
                Some(Token::RParen) => {
                    self.next();
                    break;
                }
                _ => return Err(DbError::SqlError("期望逗号或右括号".to_string())),
            }
        }
        rows.push(first_row);
        
        // 检查是否是多行插入
        let next_token = self.peek().cloned();
        if let Some(Token::Comma) = next_token {
            // 多行插入
            while let Some(Token::Comma) = self.peek().cloned() {
                self.next(); // 消费逗号
                
                // 可能有些空白，但我们期望接下来是左括号
                let next_token = self.peek().cloned();
                if let Some(Token::LParen) = next_token {
                    self.next(); // 消费左括号
                    
                    let mut row_values = Vec::new();
                    loop {
                        let value = self.parse_value()?;
                        row_values.push(value);

                        let next_token = self.peek().cloned();
                        match next_token {
                            Some(Token::Comma) => {
                                self.next();
                                continue;
                            }
                            Some(Token::RParen) => {
                                self.next();
                                break;
                            }
                            _ => return Err(DbError::SqlError("期望逗号或右括号".to_string())),
                        }
                    }
                    rows.push(row_values);
                } else {
                    return Err(DbError::SqlError("多行插入时期望左括号".to_string()));
                }
            }
            
            // 返回带列名的多行插入或普通多行插入
            if let Some(cols) = columns {
                return Ok(SqlStatement::InsertWithColumns { 
                    table, 
                    columns: cols, 
                    rows 
                });
            } else {
                return Ok(SqlStatement::InsertMultiple { table, rows });
            }
        } else {
            // 单行插入
            if let Some(cols) = columns {
                return Ok(SqlStatement::InsertWithColumns { 
                    table, 
                    columns: cols, 
                    rows: vec![rows[0].clone()] 
                });
            } else {
                return Ok(SqlStatement::Insert { table, values: rows[0].clone() });
            }
        }
    }

    fn parse_value(&mut self) -> Result<DataType, DbError> {
        match self.next() {
            Some(Token::Number(n)) => Ok(DataType::Int(n)),
            Some(Token::String(s)) => Ok(DataType::Varchar(s)),
            Some(Token::Null) => Ok(DataType::Null),
            Some(Token::Identifier(ident)) if ident.to_uppercase() == "NULL" => Ok(DataType::Null),
            _ => Err(DbError::SqlError("期望值".to_string())),
        }
    }

    fn parse_update(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Update)?;
        
        let table = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        self.expect(Token::Set)?;
        
        let mut set = Vec::new();
        loop {
            let column = match self.next() {
                Some(Token::Identifier(name)) => name,
                _ => return Err(DbError::SqlError("期望列名".to_string())),
            };

            self.expect(Token::Eq)?;
            let value = self.parse_value()?;
            set.push((column, value));

            match self.peek() {
                Some(&Token::Comma) => {
                    self.next();
                    continue;
                }
                Some(&Token::Where) => break,
                _ => return Err(DbError::SqlError("期望逗号或WHERE子句".to_string())),
            }
        }

        let where_clause = if matches!(self.peek(), Some(&Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        Ok(SqlStatement::Update { table, set, where_clause })
    }

    fn parse_delete(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Delete)?;
        self.expect(Token::From)?;
        
        let table = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        let where_clause = if matches!(self.peek(), Some(&Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        Ok(SqlStatement::Delete { table, where_clause })
    }

    fn parse_expression_select(&mut self, original_sql: &str) -> Result<SqlStatement, DbError> {
        let mut expressions = Vec::new();
        
        // 解析第一个表达式
        let expr = self.parse_expression()?;
        expressions.push(expr);
        
        // 检查是否有更多的表达式 (以逗号分隔)
        while let Some(Token::Comma) = self.peek().cloned() {
            self.next(); // 消费逗号
            let expr = self.parse_expression()?;
            expressions.push(expr);
        }
        
        // 表达式查询不能有 FROM 子句
        if let Some(Token::From) = self.peek().cloned() {
            return Err(DbError::SqlError("表达式查询不能有 FROM 子句".to_string()));
        }
        
        Ok(SqlStatement::SelectExpression { 
            expressions,
            original_sql: original_sql.to_string()
        })
    }
    
    fn parse_expression(&mut self) -> Result<super::Expression, DbError> {
        self.parse_binary_expression()
    }
    
    fn parse_binary_expression(&mut self) -> Result<super::Expression, DbError> {
        let left = self.parse_primary_expression()?;
        
        // 检查是否有运算符，先获取token的拷贝避免借用冲突
        let next_token = self.peek().cloned();
        
        match next_token {
            Some(Token::Plus) => {
                self.next(); // 消费 +
                let right = self.parse_expression()?;
                Ok(super::Expression::Binary {
                    left: Box::new(left),
                    operator: super::ArithmeticOperator::Add,
                    right: Box::new(right),
                })
            },
            Some(Token::Minus) => {
                self.next(); // 消费 -
                let right = self.parse_expression()?;
                Ok(super::Expression::Binary {
                    left: Box::new(left),
                    operator: super::ArithmeticOperator::Subtract,
                    right: Box::new(right),
                })
            },
            Some(Token::Asterisk) => {
                self.next(); // 消费 *
                let right = self.parse_expression()?;
                Ok(super::Expression::Binary {
                    left: Box::new(left),
                    operator: super::ArithmeticOperator::Multiply,
                    right: Box::new(right),
                })
            },
            Some(Token::Slash) => {
                self.next(); // 消费 /
                let right = self.parse_expression()?;
                Ok(super::Expression::Binary {
                    left: Box::new(left),
                    operator: super::ArithmeticOperator::Divide,
                    right: Box::new(right),
                })
            },
            _ => Ok(left),
        }
    }
    
    fn parse_primary_expression(&mut self) -> Result<super::Expression, DbError> {
        // 先获取当前token的拷贝而不是引用，避免借用冲突
        let current_token = self.peek().cloned();
        
        match current_token {
            Some(Token::Number(n)) => {
                self.next(); // 消费数字
                Ok(super::Expression::Literal(crate::core::types::DataType::Int(n)))
            },
            Some(Token::String(s)) => {
                self.next(); // 消费字符串
                Ok(super::Expression::Literal(crate::core::types::DataType::Varchar(s)))
            },
            Some(Token::Identifier(name)) => {
                self.next(); // 消费标识符
                Ok(super::Expression::Column(name))
            },
            Some(Token::LParen) => {
                self.next(); // 消费左括号
                let expr = self.parse_expression()?;
                self.expect(Token::RParen)?;
                Ok(expr)
            },
            _ => Err(DbError::SqlError("期望表达式".to_string())),
        }
    }
    
    fn parse_normal_select(&mut self, original_sql: &str) -> Result<SqlStatement, DbError> {
        // 检查是否为星号(*)
        if let Some(&Token::Asterisk) = self.peek() {
            self.next(); // 消耗星号
            
            self.expect(Token::From)?;
            let table = match self.next() {
                Some(Token::Identifier(name)) => name,
                _ => return Err(DbError::SqlError("期望表名".to_string())),
            };

            let where_clause = if matches!(self.peek(), Some(&Token::Where)) {
                Some(self.parse_where_clause()?)
            } else {
                None
            };

            // 解析 ORDER BY 子句
            let order_by = self.parse_order_by()?;

            return Ok(SqlStatement::Select { 
                columns: vec!["*".to_string()], 
                table, 
                where_clause,
                order_by,
            });
        }
        
        // 解析列表达式或列名
        let mut columns = Vec::new();
        let mut expressions = Vec::new();
        let mut has_expression = false;
        
        loop {
            // 保存当前位置以便回溯
            let current_position = self.position;
            
            // 尝试解析为表达式
            match self.parse_expression() {
                Ok(expr) => {
                    has_expression = true;
                    expressions.push(expr);
                },
                Err(_) => {
                    // 解析失败，回溯位置
                    self.position = current_position;
                    
                    // 尝试解析为普通列名
                    let column = match self.next() {
                        Some(Token::Identifier(name)) => name,
                        Some(Token::String(s)) => s,
                        _ => return Err(DbError::SqlError("期望列名或表达式".to_string())),
                    };
                    columns.push(column);
                }
            }

            match self.peek() {
                Some(&Token::Comma) => {
                    self.next();
                    continue;
                }
                Some(&Token::From) => break,
                _ => return Err(DbError::SqlError("期望逗号或FROM子句".to_string())),
            }
        }

        self.expect(Token::From)?;
        
        let table = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        let where_clause = if matches!(self.peek(), Some(&Token::Where)) {
            Some(self.parse_where_clause()?)
        } else {
            None
        };

        // 解析 ORDER BY 子句
        let order_by = self.parse_order_by()?;

        // 如果有表达式，将所有列名转换为Column表达式
        if has_expression {
            // 将普通列名转换为Column表达式
            for col in columns {
                expressions.push(super::Expression::Column(col));
            }
            
            Ok(SqlStatement::SelectWithExpressions { 
                expressions, 
                table, 
                where_clause,
                order_by,
                original_sql: original_sql.to_string()
            })
        } else {
            Ok(SqlStatement::Select { 
                columns, 
                table, 
                where_clause,
                order_by,
            })
        }
    }

    fn parse_where_clause(&mut self) -> Result<super::WhereClause, DbError> {
        self.expect(Token::Where)?;
        
        self.parse_or_condition()
    }

    fn parse_or_condition(&mut self) -> Result<super::WhereClause, DbError> {
        let left = self.parse_and_condition()?;

        // 检查是否有 OR 关键字
        if let Some(&Token::Or) = self.peek() {
            self.next(); // 消费 OR
            let right = self.parse_or_condition()?;
            return Ok(super::WhereClause::Or {
                left: Box::new(left),
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_and_condition(&mut self) -> Result<super::WhereClause, DbError> {
        let left = self.parse_condition()?;

        // 检查是否有 AND 关键字
        if let Some(&Token::And) = self.peek() {
            self.next(); // 消费 AND
            let right = self.parse_and_condition()?;
            return Ok(super::WhereClause::And {
                left: Box::new(left),
                right: Box::new(right),
            });
        }

        Ok(left)
    }

    fn parse_condition(&mut self) -> Result<super::WhereClause, DbError> {
        // 处理括号中的条件
        if let Some(&Token::LParen) = self.peek() {
            self.next(); // 消费左括号
            let condition = self.parse_or_condition()?;
            self.expect(Token::RParen)?;
            return Ok(condition);
        }

        // 解析简单条件
        let column = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望列名".to_string())),
        };

        // 处理IS NULL和IS NOT NULL的情况
        if let Some(&Token::Is) = self.peek() {
            self.next(); // 消费IS
            
            // 检查是否有NOT
            let is_not = if let Some(&Token::Identifier(ref ident)) = self.peek() {
                if ident.to_uppercase() == "NOT" {
                    self.next(); // 消费NOT
                    true
                } else {
                    false
                }
            } else {
                false
            };
            
            // 期望NULL
            match self.next() {
                Some(Token::Null) => {
                    // 根据是否有NOT返回不同的操作符
                    let operator = if is_not {
                        super::Operator::IsNotNull
                    } else {
                        super::Operator::IsNull
                    };
                    
                    // IS NULL条件不需要值，但为了保持一致性，使用Null
                    return Ok(super::WhereClause::Simple { 
                        column, 
                        operator, 
                        value: crate::core::types::DataType::Null 
                    });
                }
                _ => return Err(DbError::SqlError("期望NULL关键字".to_string())),
            }
        }

        let operator = match self.next() {
            Some(Token::Eq) => super::Operator::Eq,
            Some(Token::Ne) => super::Operator::Ne,
            Some(Token::Gt) => super::Operator::Gt,
            Some(Token::Lt) => super::Operator::Lt,
            Some(Token::Ge) => super::Operator::Ge,
            Some(Token::Le) => super::Operator::Le,
            _ => return Err(DbError::SqlError("期望操作符".to_string())),
        };

        let value = self.parse_value()?;

        Ok(super::WhereClause::Simple { column, operator, value })
    }

    fn parse_order_by(&mut self) -> Result<Option<super::OrderBy>, DbError> {
        // 检查是否有 ORDER BY 关键字
        if let Some(&Token::Order) = self.peek() {
            self.next(); // 消费 ORDER
            self.expect(Token::By)?; // 消费 BY

            // 获取排序列名
            let column = match self.next() {
                Some(Token::Identifier(name)) => name,
                _ => return Err(DbError::SqlError("期望列名".to_string())),
            };

            // 获取排序方向（可选）
            let direction = match self.peek() {
                Some(&Token::Asc) => {
                    self.next(); // 消费 ASC
                    super::SortDirection::Asc
                },
                Some(&Token::Desc) => {
                    self.next(); // 消费 DESC
                    super::SortDirection::Desc
                },
                _ => super::SortDirection::Asc, // 默认升序
            };

            return Ok(Some(super::OrderBy { column, direction }));
        }

        // 如果没有 ORDER BY 子句，返回 None
        Ok(None)
    }

    fn expect(&mut self, expected: Token) -> Result<(), DbError> {
        match self.next() {
            Some(token) if token == expected => Ok(()),
            Some(token) => Err(DbError::SqlError(format!("期望 {:?}, 实际 {:?}", expected, token))),
            None => Err(DbError::SqlError(format!("期望 {:?}, 但已到结尾", expected))),
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.position)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.position).cloned();
        self.position += 1;
        token
    }
} 