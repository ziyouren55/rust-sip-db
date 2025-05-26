pub mod core;
pub mod cli;

pub use core::db::{Database, StorageType};
use std::path::PathBuf;
use std::io::{self, Write};
use crate::core::error::DbError;

/// 执行SQL语句的统一接口
/// 
/// # 参数
/// * `sql_statement` - 要执行的SQL语句
/// * `db_path` - 可选的数据库路径，如果不提供则使用内存存储
/// 
/// # 返回值
/// * `bool` - 执行成功返回true，失败返回false
pub fn execute_sql(sql_statement: &str, db_path: Option<PathBuf>) -> bool {
    // 创建数据库实例
    let storage_type = match db_path {
        Some(path) => StorageType::File(path),
        None => StorageType::Memory,
    };
    
    let mut db = Database::new(storage_type);
    
    // 执行SQL语句
    match db.execute_sql(sql_statement) {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// 获取默认数据库路径
pub fn get_default_db_path() -> PathBuf {
    // 使用当前目录
    PathBuf::from("db")
}

/// 运行交互式Shell
pub fn run_interactive_shell(db: &mut Database) -> Result<(), Box<dyn std::error::Error>> {
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
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
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
                println!("  INSERT INTO table_name VALUES (1, 'value1');  -- 可以使用单引号");
                println!("  INSERT INTO table_name VALUES (2, \"value2\");  -- 或双引号");
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
            _ => handle_sql_input(input, &mut sql_buffer, &mut is_continuation)?
        }

        // 检查SQL缓冲区是否包含分号，表示SQL语句结束
        if sql_buffer.contains(';') {
            process_sql_statements(db, &mut sql_buffer, &mut is_continuation)?;
        } else {
            // 没有分号，继续接受输入
            is_continuation = true;
        }
    }

    Ok(())
}

/// 处理SQL输入
fn handle_sql_input(input: &str, sql_buffer: &mut String, is_continuation: &mut bool) -> Result<(), Box<dyn std::error::Error>> {
    // 将输入添加到SQL缓冲区
    if !input.trim_start().starts_with("--") {
        // 检查是否是多行注释的开始
        if input.trim_start().starts_with("/*") {
            // 这是多行注释的开始
            let comment = input.trim_start().trim_start_matches("/*").to_string();
            
            // 检查单行内是否完成了多行注释
            if comment.contains("*/") {
                // println!("注释: {}", comment.split("*/").next().unwrap_or("").trim());
            } else {
                // 不完整的多行注释，需要继续读取
                // println!("多行注释开始: {}", comment.trim());
                sql_buffer.push_str(input);
                sql_buffer.push('\n');
            }
        } else if input.contains("*/") && sql_buffer.contains("/*") {
            // 多行注释的结束
            // println!("多行注释结束: {}", input.split("*/").next().unwrap_or("").trim());
            sql_buffer.push_str(input);
            sql_buffer.push('\n');
        } else {
            // 如果不是注释行，直接添加
            sql_buffer.push_str(input);
            sql_buffer.push('\n'); // 添加换行符，保持格式
        }
    } else {
        // 如果是单行注释，打印出来但不添加到SQL缓冲区
        // println!("注释: {}", input.trim_start().trim_start_matches("--").trim());
    }

    Ok(())
}

/// 处理SQL语句
fn process_sql_statements(db: &mut Database, sql_buffer: &mut String, is_continuation: &mut bool) -> Result<(), Box<dyn std::error::Error>> {
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
            // println!("执行SQL: {}", stmt);
            if let Err(e) = db.execute_sql(&format!("{};", stmt)) {
                println!("{}", e);
            }
        }
        
        // 如果是最后一个语句，且不是以分号结尾，则保留在缓冲区中
        if i == statements.len() - 1 && !ends_with_semicolon && !stmt.is_empty() {
            *sql_buffer = stmt.clone();
            *is_continuation = true;
        }
    }
    
    // 如果以分号结尾或缓冲区为空，则不需要继续输入
    if ends_with_semicolon || sql_buffer.is_empty() {
        *is_continuation = false;
    }

    Ok(())
}
