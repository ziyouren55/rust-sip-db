use simple_db::core::db::{Database, StorageType};
use std::path::PathBuf;

fn main() {
    // 初始化数据库
    let mut db = Database::new(StorageType::File(PathBuf::from("db.json")));
    
    // 加载数据库
    if let Err(e) = db.load() {
        println!("加载数据库失败: {}", e);
        return;
    }
    
    println!("\n=== 测试 SELECT * 功能 ===");
    
    // 测试 SELECT * 查询
    let test_commands = [
        "SELECT * FROM users",
        "SELECT * FROM users WHERE id = 1",
        "SELECT * FROM orders",
        "SELECT * FROM products"
    ];
    
    for &cmd in &test_commands {
        println!("\n执行: {}", cmd);
        match db.execute_sql(cmd) {
            Ok(_) => println!("执行成功"),
            Err(e) => println!("执行失败: {}", e),
        }
    }
} 