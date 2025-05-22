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

    pub fn parse(&mut self, tokens: Vec<Token>) -> Result<SqlStatement, DbError> {
        self.tokens = tokens;
        self.position = 0;
        self.parse_statement()
    }

    fn parse_statement(&mut self) -> Result<SqlStatement, DbError> {
        match self.peek() {
            Some(Token::Create) => self.parse_create_table(),
            Some(Token::Drop) => self.parse_drop_table(),
            Some(Token::Insert) => self.parse_insert(),
            Some(Token::Update) => self.parse_update(),
            Some(Token::Delete) => self.parse_delete(),
            Some(Token::Select) => self.parse_select(),
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
        match self.peek() {
            Some(Token::Identifier(ident)) if ident.to_uppercase() == "NOT" => {
                self.next();
                self.expect(Token::Identifier("NULL".to_string()))?;
                Ok(false)
            }
            _ => Ok(true),
        }
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
        
        let name = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        Ok(SqlStatement::DropTable { name })
    }

    fn parse_insert(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Insert)?;
        self.expect(Token::Into)?;
        
        let table = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望表名".to_string())),
        };

        self.expect(Token::Values)?;
        self.expect(Token::LParen)?;
        
        let mut values = Vec::new();
        loop {
            let value = self.parse_value()?;
            values.push(value);

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

        Ok(SqlStatement::Insert { table, values })
    }

    fn parse_value(&mut self) -> Result<DataType, DbError> {
        match self.next() {
            Some(Token::Number(n)) => Ok(DataType::Int(n)),
            Some(Token::String(s)) => Ok(DataType::Varchar(s)),
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

    fn parse_select(&mut self) -> Result<SqlStatement, DbError> {
        self.expect(Token::Select)?;
        
        let mut columns = Vec::new();
        
        // 检查是否为星号(*)
        if let Some(&Token::Star) = self.peek() {
            self.next(); // 消耗星号
            columns.push("*".to_string()); // 使用星号字符串表示所有列
        } else {
            // 常规列名列表处理
            loop {
                let column = match self.next() {
                    Some(Token::Identifier(name)) => name,
                    Some(Token::String(s)) => s,
                    _ => return Err(DbError::SqlError("期望列名或字符串".to_string())),
                };
                columns.push(column);

                match self.peek() {
                    Some(&Token::Comma) => {
                        self.next();
                        continue;
                    }
                    Some(&Token::From) => break,
                    _ => return Err(DbError::SqlError("期望逗号或FROM子句".to_string())),
                }
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

        Ok(SqlStatement::Select { columns, table, where_clause })
    }

    fn parse_where_clause(&mut self) -> Result<super::WhereClause, DbError> {
        self.expect(Token::Where)?;
        
        let column = match self.next() {
            Some(Token::Identifier(name)) => name,
            _ => return Err(DbError::SqlError("期望列名".to_string())),
        };

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

        Ok(super::WhereClause { column, operator, value })
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