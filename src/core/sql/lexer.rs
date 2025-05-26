use crate::core::error::DbError;

#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    // 关键字
    Create,
    Table,
    Drop,
    Insert,
    Into,
    Values,
    Update,
    Set,
    Delete,
    From,
    Where,
    Select,
    Primary,
    Key,
    And,    // 新增 AND 关键字
    Or,     // 新增 OR 关键字
    Is,     // 新增 IS 关键字
    Null,   // 新增 NULL 关键字
    Order,  // ORDER BY 子句的 ORDER
    By,     // ORDER BY 子句的 BY
    Asc,    // 升序排序
    Desc,   // 降序排序
    // 操作符
    Eq,    // =
    Ne,    // !=
    Gt,    // >
    Lt,    // <
    Ge,    // >=
    Le,    // <=
    // 算术运算符
    Plus,     // +
    Minus,    // -
    Asterisk, // * (也用于SELECT * 查询)
    Slash,    // /
    // 分隔符
    Comma,     // ,
    Semicolon, // ;
    LParen,    // (
    RParen,    // )
    Star,      // *
    // 字面量
    Identifier(String),
    String(String),
    Number(i32),
    // 其他
    Comment(String),
    MultiLineComment(String), // 新增的多行注释类型
}

pub struct Lexer {
    input: String,
    position: usize,
}

impl Lexer {
    pub fn new() -> Self {
        Lexer {
            input: String::new(),
            position: 0,
        }
    }

    pub fn tokenize(&mut self, input: &str) -> Result<Vec<Token>, DbError> {
        self.input = input.to_string();
        self.position = 0;
        let mut tokens = Vec::new();

        while self.position < self.input.len() {
            // 安全地获取当前字符，避免使用unwrap
            let c = match self.input.chars().nth(self.position) {
                Some(ch) => ch,
                None => break, // 如果没有字符了，结束循环
            };
            
            // 跳过空白字符
            if c.is_whitespace() {
                self.position += 1;
                continue;
            }

            // 处理单行注释
            if c == '-' && self.peek() == Some('-') {
                self.position += 2;
                let comment = self.read_until('\n');
                tokens.push(Token::Comment(comment));
                continue;
            }

            // 处理多行注释 /* ... */
            if c == '/' && self.peek() == Some('*') {
                self.position += 2; // 跳过 /*
                let comment = self.read_until_multiline_comment_end();
                tokens.push(Token::MultiLineComment(comment));
                continue;
            }

            // 处理标识符和关键字
            if c.is_alphabetic() {
                let identifier = self.read_identifier();
                let token = match identifier.to_uppercase().as_str() {
                    "CREATE" => Token::Create,
                    "TABLE" => Token::Table,
                    "DROP" => Token::Drop,
                    "INSERT" => Token::Insert,
                    "INTO" => Token::Into,
                    "VALUES" => Token::Values,
                    "UPDATE" => Token::Update,
                    "SET" => Token::Set,
                    "DELETE" => Token::Delete,
                    "FROM" => Token::From,
                    "WHERE" => Token::Where,
                    "SELECT" => Token::Select,
                    "PRIMARY" => Token::Primary,
                    "KEY" => Token::Key,
                    "AND" => Token::And,    // 新增 AND 关键字识别
                    "OR" => Token::Or,      // 新增 OR 关键字识别
                    "IS" => Token::Is,      // 新增 IS 关键字识别
                    "NULL" => Token::Null,  // 新增 NULL 关键字识别
                    "ORDER" => Token::Order, // ORDER BY 子句的 ORDER
                    "BY" => Token::By,       // ORDER BY 子句的 BY
                    "ASC" => Token::Asc,     // 升序排序
                    "DESC" => Token::Desc,   // 降序排序
                    _ => Token::Identifier(identifier),
                };
                tokens.push(token);
                continue;
            }

            // 处理数字
            if c.is_digit(10) {
                let number = self.read_number();
                tokens.push(Token::Number(number));
                continue;
            }

            // 处理字符串 - 支持单引号和双引号
            if c == '\'' || c == '"' {
                let quote_char = c; // 记住是哪种引号
                self.position += 1;
                let string = self.read_until(quote_char);
                // 安全地移动位置，避免越界
                if self.position < self.input.len() {
                self.position += 1;
                }
                tokens.push(Token::String(string));
                continue;
            }

            // 处理操作符和分隔符
            let token = match c {
                '=' => Token::Eq,
                '!' if self.peek() == Some('=') => {
                    self.position += 1;
                    Token::Ne
                }
                '>' if self.peek() == Some('=') => {
                    self.position += 1;
                    Token::Ge
                }
                '>' => Token::Gt,
                '<' if self.peek() == Some('=') => {
                    self.position += 1;
                    Token::Le
                }
                '<' => Token::Lt,
                ',' => Token::Comma,
                ';' => Token::Semicolon,
                '(' => Token::LParen,
                ')' => Token::RParen,
                '*' => Token::Asterisk,
                '+' => Token::Plus,
                '-' => Token::Minus,
                '/' => Token::Slash,
                _ => return Err(DbError::SqlError(format!("未知字符: {}", c))),
            };
            tokens.push(token);
            self.position += 1;
        }

        Ok(tokens)
    }

    fn peek(&self) -> Option<char> {
        if self.position + 1 < self.input.len() {
            self.input.chars().nth(self.position + 1)
        } else {
            None
        }
    }

    fn read_identifier(&mut self) -> String {
        let mut identifier = String::new();
        while self.position < self.input.len() {
            // 安全地获取当前字符
            let c = match self.input.chars().nth(self.position) {
                Some(ch) => ch,
                None => break, // 如果没有更多字符，跳出循环
            };
            
            if c.is_alphanumeric() || c == '_' {
                identifier.push(c);
                self.position += 1;
            } else {
                break;
            }
        }
        identifier
    }

    fn read_number(&mut self) -> i32 {
        let mut number = String::new();
        while self.position < self.input.len() {
            // 安全地获取当前字符
            let c = match self.input.chars().nth(self.position) {
                Some(ch) => ch,
                None => break, // 如果没有更多字符，跳出循环
            };
            
            if c.is_digit(10) {
                number.push(c);
                self.position += 1;
            } else {
                break;
            }
        }
        // 安全地解析数字，如果解析失败返回0（实际应用中可能需要更好的错误处理）
        number.parse().unwrap_or(0)
    }

    fn read_until(&mut self, end: char) -> String {
        let mut result = String::new();
        while self.position < self.input.len() {
            // 安全地获取当前字符
            let c = match self.input.chars().nth(self.position) {
                Some(ch) => ch,
                None => break, // 如果没有更多字符，跳出循环
            };
            
            if c == end {
                break;
            }
            result.push(c);
            self.position += 1;
        }
        result
    }
    
    // 读取多行注释，直到遇到 */
    fn read_until_multiline_comment_end(&mut self) -> String {
        let mut result = String::new();
        
        while self.position + 1 < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap_or(' ');
            let next = self.input.chars().nth(self.position + 1).unwrap_or(' ');
            
            if c == '*' && next == '/' {
                self.position += 2; // 跳过 */
                break;
            }
            
            result.push(c);
            self.position += 1;
        }
        
        result
    }
} 