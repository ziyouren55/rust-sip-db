use clap::Parser;
use std::io::Write;
use crate::core::db::{Database, StorageType};
use crate::core::error::DbError;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
pub struct Cli {
    #[arg(short, long, default_value = "db.json")]
    db_path: String,
}

impl Cli {
    pub fn run(&mut self) -> Result<(), DbError> {
        let mut db = Database::new(StorageType::File(PathBuf::from(&self.db_path)));
        db.load()?;

        println!("SimpleDB - 一个简单的数据库实现");
        println!("输入 'help' 获取帮助信息");
        println!("输入 'exit' 退出程序");
        println!("SQL语句以分号(;)结束，可以跨多行输入");
        println!("支持以--开头的SQL注释");

        // 用于缓存多行SQL语句
        let mut sql_buffer = String::new();
        // 记录提示符状态
        let mut is_continuation = false;

        loop {
            // 根据是否在继续输入SQL语句显示不同的提示符
            if is_continuation {
                print!("-> ");
            } else {
                print!("> ");
            }
            std::io::stdout().flush()?;

            let mut input = String::new();
            std::io::stdin().read_line(&mut input)?;
            let input = input.trim();

            if input.is_empty() {
                continue;
            }

            // 处理特殊命令，这些命令不需要分号
            match input {
                "exit" => break,
                "help" => {
                    println!("可用命令:");
                    println!("  help - 显示帮助信息");
                    println!("  exit - 退出程序");
                    println!("  list - 列出所有表");
                    println!("  save - 保存数据库");
                    println!("  load - 加载数据库");
                    println!("  clear - 清除当前SQL缓冲区");
                    println!("SQL命令: (以分号结束)");
                    println!("  -- 这是SQL注释");
                    println!("  CREATE TABLE table_name (column1 type1, column2 type2, ...);");
                    println!("  DROP TABLE table_name;");
                    println!("  INSERT INTO table_name VALUES (value1, value2, ...);");
                    println!("  UPDATE table_name SET column = value WHERE condition;");
                    println!("  DELETE FROM table_name WHERE condition;");
                    println!("  SELECT * FROM table_name WHERE condition;");
                    is_continuation = false;
                    sql_buffer.clear();
                    continue;
                },
                "list" => {
                    let tables = db.list_tables()?;
                    if tables.is_empty() {
                        println!("没有表");
                    } else {
                        println!("表列表:");
                        for table in tables {
                            println!("  {}", table);
                        }
                    }
                    is_continuation = false;
                    sql_buffer.clear();
                    continue;
                },
                "save" => {
                    db.save()?;
                    println!("数据库已保存");
                    is_continuation = false;
                    sql_buffer.clear();
                    continue;
                },
                "load" => {
                    db.load()?;
                    println!("数据库已加载");
                    is_continuation = false;
                    sql_buffer.clear();
                    continue;
                },
                "clear" => {
                    // 添加清除当前输入缓冲区的命令
                    println!("已清除当前SQL缓冲区");
                    is_continuation = false;
                    sql_buffer.clear();
                    continue;
                },
                _ => {
                    // 将输入添加到SQL缓冲区
                    sql_buffer.push_str(input);
                    sql_buffer.push(' '); // 添加空格，保持格式
                }
            }

            // 检查SQL缓冲区是否包含分号，表示SQL语句结束
            if sql_buffer.contains(';') {
                // 拆分SQL语句（可能有多个语句用分号分隔）
                // 将分割后的语句复制到一个新的向量，避免对sql_buffer的借用
                let statements: Vec<String> = sql_buffer.split(';')
                    .map(|s| s.trim().to_string())
                    .collect();
                
                // 判断是否以分号结尾
                let ends_with_semicolon = sql_buffer.trim().ends_with(';');
                
                // 清空SQL缓冲区，这样就不会有借用问题
                sql_buffer.clear();
                
                // 处理所有非空语句
                for (i, stmt) in statements.iter().enumerate() {
                    if !stmt.is_empty() {
                        // 显示要执行的SQL语句（不包括注释）
                        let display_stmt = stmt.lines()
                            .filter(|line| !line.trim_start().starts_with("--"))
                            .collect::<Vec<&str>>()
                            .join("\n");
                        
                        if !display_stmt.trim().is_empty() {
                            println!("执行SQL: {}", display_stmt);
                            if let Err(e) = db.execute_sql(&format!("{};", stmt)) {
                                println!("错误: {}", e);
                            }
                        } else {
                            println!("跳过仅包含注释的语句");
                        }
                    }
                    
                    // 如果是最后一个语句，且不是以分号结尾，则保留在缓冲区中
                    if i == statements.len() - 1 && !ends_with_semicolon && !stmt.is_empty() {
                        sql_buffer = stmt.clone();
                        is_continuation = true;
                    }
                }
                
                // 如果以分号结尾或缓冲区为空，则不需要继续输入
                if ends_with_semicolon || sql_buffer.is_empty() {
                    is_continuation = false;
                }
            } else {
                // 没有分号，继续接受输入
                is_continuation = true;
            }
        }

        Ok(())
    }
} 