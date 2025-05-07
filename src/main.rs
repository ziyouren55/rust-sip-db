use std::io::{self, Write};
use simple_db::core::db::{Database, StorageType};
use std::path::PathBuf;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("SimpleDB - 一个简单的数据库实现");
    println!("输入 'help' 获取帮助信息");
    println!("输入 'exit' 退出程序");

    let mut db = Database::new(StorageType::File(PathBuf::from("db.json")));
    db.load()?;

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let input = input.trim();

        if input.is_empty() {
            continue;
        }

        match input {
            "exit" => break,
            "help" => {
                println!("可用命令:");
                println!("  help - 显示帮助信息");
                println!("  exit - 退出程序");
                println!("  list - 列出所有表");
                println!("  save - 保存数据库");
                println!("  load - 加载数据库");
                println!("SQL命令:");
                println!("  CREATE TABLE table_name (column1 type1, column2 type2, ...)");
                println!("  DROP TABLE table_name");
                println!("  INSERT INTO table_name VALUES (value1, value2, ...)");
                println!("  UPDATE table_name SET column = value WHERE condition");
                println!("  DELETE FROM table_name WHERE condition");
                println!("  SELECT * FROM table_name WHERE condition");
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
            },
            "save" => {
                db.save()?;
                println!("数据库已保存");
            },
            "load" => {
                db.load()?;
                println!("数据库已加载");
            },
            _ => {
                if let Err(e) = db.execute_sql(input) {
                    println!("错误: {}", e);
                }
            }
        }
    }

    Ok(())
}
