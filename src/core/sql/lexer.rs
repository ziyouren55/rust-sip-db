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
    // 操作符
    Eq,    // =
    Ne,    // !=
    Gt,    // >
    Lt,    // <
    Ge,    // >=
    Le,    // <=
    // 分隔符
    Comma,     // ,
    Semicolon, // ;
    LParen,    // (
    RParen,    // )
    // 字面量
    Identifier(String),
    String(String),
    Number(i32),
    // 其他
    Comment(String),
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
            let c = self.input.chars().nth(self.position).unwrap();
            
            // 跳过空白字符
            if c.is_whitespace() {
                self.position += 1;
                continue;
            }

            // 处理注释
            if c == '-' && self.peek() == Some('-') {
                self.position += 2;
                let comment = self.read_until('\n');
                tokens.push(Token::Comment(comment));
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

            // 处理字符串
            if c == '\'' {
                self.position += 1;
                let string = self.read_until('\'');
                self.position += 1;
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
            let c = self.input.chars().nth(self.position).unwrap();
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
            let c = self.input.chars().nth(self.position).unwrap();
            if c.is_digit(10) {
                number.push(c);
                self.position += 1;
            } else {
                break;
            }
        }
        number.parse().unwrap()
    }

    fn read_until(&mut self, end: char) -> String {
        let mut result = String::new();
        while self.position < self.input.len() {
            let c = self.input.chars().nth(self.position).unwrap();
            if c == end {
                break;
            }
            result.push(c);
            self.position += 1;
        }
        result
    }
} 