use std::io::{self, Write};
use simple_db::core::db::{Database, StorageType};
use simple_db::{get_default_db_path, run_interactive_shell};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("SimpleDB - 一个简单的数据库实现");
    println!("输入 'help' 获取帮助信息");
    println!("输入 'exit' 退出程序");
    
    // 使用当前目录作为数据库目录
    let db_path = get_default_db_path();
    println!("数据库存储目录: {}", db_path.display());

    let mut db = Database::new(StorageType::File(db_path));
    db.load()?;

    // 运行交互式shell
    run_interactive_shell(&mut db)?;

    Ok(())
}
