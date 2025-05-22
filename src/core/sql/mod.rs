mod lexer;
mod parser;
mod executor;
mod formatter;

pub use lexer::{Token, Lexer};
pub use parser::Parser;
pub use executor::SqlExecutor;
pub use formatter::TableFormatter;

use crate::core::error::DbError;
use crate::core::types::{DataType, Column};
use crate::core::storage::Storage;

// SQL语句类型
#[derive(Debug)]
pub enum SqlStatement {
    CreateTable {
        name: String,
        columns: Vec<Column>,
    },
    DropTable {
        name: String,
    },
    Insert {
        table: String,
        values: Vec<DataType>,
    },
    Update {
        table: String,
        set: Vec<(String, DataType)>,
        where_clause: Option<WhereClause>,
    },
    Delete {
        table: String,
        where_clause: Option<WhereClause>,
    },
    Select {
        columns: Vec<String>,
        table: String,
        where_clause: Option<WhereClause>,
    },
}

// WHERE子句
#[derive(Debug)]
pub struct WhereClause {
    pub column: String,
    pub operator: Operator,
    pub value: DataType,
}

// 操作符
#[derive(Debug, PartialEq)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Lt,
    Ge,
    Le,
}

// SQL解析器
pub struct SqlParser {
    lexer: lexer::Lexer,
    parser: parser::Parser,
}

impl SqlParser {
    pub fn new() -> Self {
        SqlParser {
            lexer: lexer::Lexer::new(),
            parser: parser::Parser::new(),
        }
    }

    pub fn parse(&mut self, sql: &str) -> Result<SqlStatement, DbError> {
        let tokens = self.lexer.tokenize(sql)?;
        self.parser.parse(tokens)
    }
} 