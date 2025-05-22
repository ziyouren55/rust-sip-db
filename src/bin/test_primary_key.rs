use simple_db::core::db::{Database, StorageType};
use std::path::PathBuf;

fn main() {
    // 初始化数据库
    let mut db = Database::new(StorageType::Memory);
    
    println!("\n=== 测试 PRIMARY KEY 和 INT(n) 支持 ===");
    
    let test_commands = [
        // 创建带有主键和位数标注的表
        "CREATE TABLE plants (id INT(32) PRIMARY KEY, name VARCHAR(100) NOT NULL, height INT, age INT(8))",
        
        // 插入数据
        "INSERT INTO plants VALUES (1, 'Oak', 500, 100)",
        "INSERT INTO plants VALUES (2, 'Pine', 300, 50)",
        "INSERT INTO plants VALUES (3, 'Maple', 250, 30)",
        
        // 尝试插入重复主键（应该失败）
        "INSERT INTO plants VALUES (1, 'Birch', 200, 20)",
        
        // 查询数据
        "SELECT * FROM plants",
        
        // 另一个例子
        "CREATE TABLE animals (id INT PRIMARY KEY, name VARCHAR(50) NOT NULL, species VARCHAR(100))",
        "INSERT INTO animals VALUES (1, 'Leo', 'Lion')",
        "INSERT INTO animals VALUES (2, 'Max', 'Dog')",
        "SELECT * FROM animals"
    ];
    
    for (i, &cmd) in test_commands.iter().enumerate() {
        println!("\n测试 #{}: {}", i + 1, cmd);
        match db.execute_sql(cmd) {
            Ok(_) => println!("执行成功"),
            Err(e) => println!("执行失败: {}", e),
        }
    }
} 