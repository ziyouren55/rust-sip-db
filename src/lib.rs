pub mod core;

pub use core::db::{Database, ErrorDisplayMode, StorageType};
use std::io::{self, Write};
use std::path::PathBuf;

/// SQL执行结果结构体
#[derive(Debug, Clone)]
pub struct SqlResult {
    pub success: bool,        // 执行是否成功
    pub error_message: String, // 错误信息（如果有）
}

/// 执行SQL语句的带路径接口
/// 
/// # 参数
/// * `sql_statement` - 要执行的SQL语句
/// * `db_path` - 可选的数据库路径，如果不提供则使用内存存储
/// * `stop_on_error` - 是否在遇到第一个错误时立即停止执行
/// 
/// # 返回值
/// * `bool` - 执行成功返回true，失败返回false
pub fn execute_sql_with_path(sql_statement: &str, db_path: Option<PathBuf>, stop_on_error: bool) -> bool {
    // 创建数据库实例
    let storage_type = match db_path {
        Some(path) => StorageType::File(path),
        None => StorageType::Memory,
    };
    
    let mut db = Database::new(storage_type);
    let mut success = true;
    
    // 处理输入，移除注释
    let cleaned_sql = remove_comments(sql_statement);
    
    // 分割多条SQL语句
    let statements: Vec<String> = cleaned_sql.split(';')
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .collect();
    
    // 记录上一条是否有输出（用于判断是否需要添加空行）
    let mut last_had_output = false;
    // 记录是否执行了任何SELECT语句
    let mut has_executed_select = false;
    // 记录是否有任何表格输出
    let mut has_table_output = false;
    
    // 依次执行每条语句
    for stmt in statements {
        if !stmt.is_empty() {
            // 检查当前语句是否为SELECT语句
            let is_select = stmt.trim_start().to_uppercase().starts_with("SELECT");
            
            if is_select {
                has_executed_select = true;
                
                // 如果上一条也有输出，添加一个空行
                if last_had_output {
                    println!();
                }
            }
            
            match db.execute_sql_with_output(&format!("{};", stmt)) {
                Ok(has_output) => {
                    // 更新状态
                    last_had_output = has_output;
                    if has_output {
                        has_table_output = true;
                    }
                },
                Err(e) => {
                    // 使用当前错误显示模式格式化错误信息并打印
                    println!("{}", db.format_error(&e));
                    success = false;
                    last_had_output = false; // 执行失败，重置状态
                    
                    // 如果设置了遇到错误立即停止，则中断执行
                    if stop_on_error {
                        // println!("遇到错误，终止执行");
                        return false;
                    }
                }
            }
        }
    }
    
    // 如果执行了SELECT语句但没有输出
    if has_executed_select && !has_table_output {
        println!("There are no results to be displayed.");
    }
    
    success
}

/// 移除SQL语句中的注释
fn remove_comments(sql: &str) -> String {
    let mut result = String::new();
    let mut in_multi_comment = false;
    let mut in_single_comment = false;
    let mut in_string = false;
    let mut string_quote = '\0'; // 存储字符串的引号类型（单引号或双引号）
    let mut i = 0;
    
    let chars: Vec<char> = sql.chars().collect();
    
    while i < chars.len() {
        let c = chars[i];
        
        // 检查是否在字符串中
        if !in_single_comment && !in_multi_comment {
            if (c == '\'' || c == '"') && (i == 0 || chars[i-1] != '\\') {
                if !in_string {
                    in_string = true;
                    string_quote = c;
                } else if c == string_quote {
                    in_string = false;
                }
            }
        }
        
        // 处理单行注释开始 --
        if !in_string && !in_multi_comment && !in_single_comment && c == '-' && i + 1 < chars.len() && chars[i + 1] == '-' {
            in_single_comment = true;
            i += 2;
            continue;
        }
        
        // 处理多行注释开始 /*
        if !in_string && !in_single_comment && !in_multi_comment && c == '/' && i + 1 < chars.len() && chars[i + 1] == '*' {
            in_multi_comment = true;
            i += 2;
            continue;
        }
        
        // 处理多行注释结束 */
        if !in_string && !in_single_comment && in_multi_comment && c == '*' && i + 1 < chars.len() && chars[i + 1] == '/' {
            in_multi_comment = false;
            i += 2;
            continue;
        }
        
        // 处理单行注释结束（遇到换行）
        if in_single_comment && (c == '\n' || c == '\r') {
            in_single_comment = false;
        }
        
        // 只有不在注释中的内容才添加到结果中
        if !in_single_comment && !in_multi_comment {
            result.push(c);
        }
        
        i += 1;
    }
    
    result
}

/// 执行SQL语句的统一接口（使用内存存储）
/// 
/// # 参数
/// * `sql_statement` - 要执行的SQL语句
/// 
/// # 返回值
/// * `bool` - 执行成功返回true，失败返回false
pub fn execute_sql(sql_statement: &str) -> bool {
    execute_sql_with_path(sql_statement, None, false)
}

/// 获取默认数据库路径
pub fn get_default_db_path() -> PathBuf {
    // 使用当前目录
    PathBuf::from("db")
}

/// 运行交互式Shell
pub fn run_interactive_shell(db: &mut Database) -> Result<(), Box<dyn std::error::Error>> {
    println!("输入 'toggle_error_mode' 切换错误显示模式");
    
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
                println!("  toggle_error_mode - 切换错误显示模式（简略/详细）");
                println!("  error_mode - 显示当前错误显示模式");
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
            "toggle_error_mode" => {
                let mode = db.toggle_error_mode();
                match mode {
                    crate::core::db::ErrorDisplayMode::Brief => println!("错误显示模式切换为: 简略"),
                    crate::core::db::ErrorDisplayMode::Detailed => println!("错误显示模式切换为: 详细"),
                }
                is_continuation = false;
                sql_buffer.clear();
                continue;
            },
            "error_mode" => {
                let mode = db.get_error_mode();
                match mode {
                    crate::core::db::ErrorDisplayMode::Brief => println!("当前错误显示模式: 简略"),
                    crate::core::db::ErrorDisplayMode::Detailed => println!("当前错误显示模式: 详细"),
                }
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
            _ => handle_sql_input(input, &mut sql_buffer)?
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
fn handle_sql_input(input: &str, sql_buffer: &mut String) -> Result<(), Box<dyn std::error::Error>> {
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
            // 显示执行的SQL语句
            println!("执行SQL: {}", stmt);
            if let Err(e) = db.execute_sql(&format!("{};", stmt)) {
                // 使用当前错误显示模式格式化错误信息
                println!("{}", db.format_error(&e));
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

/// 运行SimpleDB，支持交互式模式和文件模式
///
/// # 参数
/// * `args` - 命令行参数，通过 std::env::args().collect::<Vec<String>>() 获取
///
/// # 返回值
/// * `Result<(), Box<dyn std::error::Error>>` - 执行结果
pub fn run_simple_db(args: Vec<String>) -> Result<(), Box<dyn std::error::Error>> {

    // 使用当前目录作为数据库目录
    let db_path = get_default_db_path();

    let mut db = Database::new(StorageType::File(db_path.clone()));
    db.load()?;

    // 检查是否提供了SQL文件参数
    if args.len() == 2 {
        // 文件模式 - 读取并执行SQL文件
        let sql_file_path = &args[1];
        // println!("执行SQL文件: {}", sql_file_path);

        // 读取文件内容
        let sql_content = std::fs::read_to_string(sql_file_path)
            .map_err(|e| format!("无法读取SQL文件: {}", e))?;

        // 执行SQL语句，脚本模式下遇到错误立即停止
        if execute_sql_with_path(&sql_content, Some(db.get_storage_path()), true) {
            // println!("SQL文件执行成功");
        } else {
            // println!("SQL文件执行过程中出现错误");
        }
    } else {
        // 交互式模式
        println!("SimpleDB - 一个简单的数据库实现");
        println!("数据库存储目录: {}", db_path.display());
        println!("输入 'help' 获取帮助信息");
        println!("输入 'exit' 退出程序");

        // 运行交互式shell
        run_interactive_shell(&mut db)?;
    }

    Ok(())
}
