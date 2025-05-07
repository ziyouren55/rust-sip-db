use simple_db::core::db::{Database, StorageType};
use std::path::PathBuf;
use std::fs;

fn main() {
    let db_path = PathBuf::from("test_persistence.json");
    
    // 如果文件存在，先删除它
    if db_path.exists() {
        fs::remove_file(&db_path).expect("无法删除旧数据库文件");
    }

    println!("=== 测试数据库持久化 ===");
    
    // 第一次运行：创建数据库并插入数据
    println!("\n1. 创建数据库并插入数据");
    {
        let mut db = Database::new(StorageType::File(db_path.clone()));
        
        // 创建表
        println!("\n创建表...");
        let commands = vec![
            "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50) NOT NULL, age INT)",
            "CREATE TABLE products (id INT NOT NULL, name VARCHAR(50) NOT NULL, price INT)",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
        
        // 插入数据
        println!("\n插入数据...");
        let commands = vec![
            "INSERT INTO users VALUES (1, 'Alice', 25)",
            "INSERT INTO users VALUES (2, 'Bob', 30)",
            "INSERT INTO products VALUES (1, 'Laptop', 999)",
            "INSERT INTO products VALUES (2, 'Mouse', 29)",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
        
        // 查询数据
        println!("\n查询数据...");
        let commands = vec![
            "SELECT id, name, age FROM users",
            "SELECT id, name, price FROM products",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
    } // 数据库在这里被关闭
    
    // 第二次运行：重新打开数据库并验证数据
    println!("\n2. 重新打开数据库并验证数据");
    {
        let mut db = Database::new(StorageType::File(db_path.clone()));
        
        // 查询数据
        println!("\n查询数据...");
        let commands = vec![
            "SELECT id, name, age FROM users",
            "SELECT id, name, price FROM products",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
        
        // 插入新数据
        println!("\n插入新数据...");
        let commands = vec![
            "INSERT INTO users VALUES (3, 'Charlie', 35)",
            "INSERT INTO products VALUES (3, 'Keyboard', 79)",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
        
        // 再次查询所有数据
        println!("\n再次查询所有数据...");
        let commands = vec![
            "SELECT id, name, age FROM users",
            "SELECT id, name, price FROM products",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
    } // 数据库再次被关闭
    
    // 第三次运行：最终验证
    println!("\n3. 最终验证");
    {
        let mut db = Database::new(StorageType::File(db_path.clone()));
        
        // 查询所有数据
        println!("\n查询所有数据...");
        let commands = vec![
            "SELECT id, name, age FROM users",
            "SELECT id, name, price FROM products",
        ];
        
        for cmd in commands {
            println!("执行: {}", cmd);
            match db.execute_sql(cmd) {
                Ok(_) => println!("成功"),
                Err(e) => println!("失败: {}", e),
            }
        }
    }
    
    // 清理：删除测试数据库文件
    if db_path.exists() {
        fs::remove_file(&db_path).expect("无法删除测试数据库文件");
        println!("\n测试数据库文件已删除");
    }
} 