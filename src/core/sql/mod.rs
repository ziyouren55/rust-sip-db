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
    DropTables {
        names: Vec<String>,
    },
    Insert {
        table: String,
        values: Vec<DataType>,
    },
    InsertMultiple {
        table: String,
        rows: Vec<Vec<DataType>>,
    },
    InsertWithColumns {
        table: String,
        columns: Vec<String>,
        rows: Vec<Vec<DataType>>,
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
        order_by: Option<OrderBy>,
    },
    SelectExpression {
        expressions: Vec<Expression>,
        original_sql: String,
    },
    SelectWithExpressions {
        expressions: Vec<Expression>,
        table: String,
        where_clause: Option<WhereClause>,
        order_by: Option<OrderBy>,
        original_sql: String,
    },
}

// WHERE子句
#[derive(Debug)]
pub enum WhereClause {
    Simple {
        column: String,
        operator: Operator,
        value: DataType,
    },
    And {
        left: Box<WhereClause>,
        right: Box<WhereClause>,
    },
    Or {
        left: Box<WhereClause>,
        right: Box<WhereClause>,
    },
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
    IsNull,
    IsNotNull,
}

// 表达式
#[derive(Debug)]
pub enum Expression {
    Literal(DataType),
    Column(String),
    Binary {
        left: Box<Expression>,
        operator: ArithmeticOperator,
        right: Box<Expression>,
    },
}

// 算术运算符
#[derive(Debug, PartialEq)]
pub enum ArithmeticOperator {
    Add,     // +
    Subtract, // -
    Multiply, // *
    Divide,   // /
}

// 排序方向
#[derive(Debug, PartialEq)]
pub enum SortDirection {
    Asc,
    Desc,
}

// 排序子句
#[derive(Debug)]
pub struct OrderBy {
    pub column: String,
    pub direction: SortDirection,
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
        self.parser.parse(tokens, sql)
    }
} 