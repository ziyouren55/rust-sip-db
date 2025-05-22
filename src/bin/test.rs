use simple_db::core::db::{Database, StorageType};
use std::path::PathBuf;

fn main() {
    // 初始化数据库
    let mut db = Database::new(StorageType::File(PathBuf::from("db.json")));
    
    // 第一组测试：基本功能测试
    println!("\n=== 基本功能测试 ===");
    let basic_tests = vec![
        // 创建多个表
        "CREATE TABLE users (id INT NOT NULL, name VARCHAR(50) NOT NULL, age INT, email VARCHAR(100))",
        "CREATE TABLE orders (order_id INT NOT NULL, user_id INT NOT NULL, product VARCHAR(50), amount INT, order_date VARCHAR(20))",
        "CREATE TABLE products (id INT NOT NULL, name VARCHAR(50) NOT NULL, price INT, stock INT)",
        
        // 插入用户数据
        "INSERT INTO users VALUES (1, 'Alice', 25, 'alice@example.com')",
        "INSERT INTO users VALUES (2, 'Bob', 30, 'bob@example.com')",
        "INSERT INTO users VALUES (3, 'Charlie', NULL, 'charlie@example.com')",
        "INSERT INTO users VALUES (4, 'David', 35, NULL)",
        
        // 插入订单数据
        "INSERT INTO orders VALUES (1, 1, 'Laptop', 999, '2024-01-15')",
        "INSERT INTO orders VALUES (2, 1, 'Mouse', 29, '2024-01-16')",
        "INSERT INTO orders VALUES (3, 2, 'Keyboard', 79, '2024-01-17')",
        "INSERT INTO orders VALUES (4, 3, 'Monitor', 299, '2024-01-18')",
        
        // 插入产品数据
        "INSERT INTO products VALUES (1, 'Laptop', 999, 10)",
        "INSERT INTO products VALUES (2, 'Mouse', 29, 50)",
        "INSERT INTO products VALUES (3, 'Keyboard', 79, 30)",
        "INSERT INTO products VALUES (4, 'Monitor', 299, 15)",
        
        // 测试基本查询
        "SELECT id, name, age, email FROM users",
        "SELECT order_id, user_id, product, amount, order_date FROM orders",
        "SELECT id, name, price, stock FROM products",
        
        // 测试条件查询
        "SELECT id, name, age, email FROM users WHERE id > 2",
        "SELECT order_id, user_id, product, amount, order_date FROM orders WHERE amount > 100",
        "SELECT id, name, price, stock FROM products WHERE stock < 20",
        
        // 测试更新操作
        "UPDATE users SET age = 26 WHERE id = 1",
        "UPDATE products SET price = 899 WHERE id = 1",
        "UPDATE orders SET amount = 899 WHERE order_id = 1",
        
        // 测试删除操作
        "DELETE FROM users WHERE age = NULL",
        "DELETE FROM orders WHERE amount < 50",
        
        // 验证最终状态
        "SELECT id, name, age, email FROM users",
        "SELECT order_id, user_id, product, amount, order_date FROM orders",
        "SELECT id, name, price, stock FROM products",

        // 测试 * 
        "SELECT * FROM users",
    ];

    // 第二组测试：高级功能测试
    println!("\n=== 高级功能测试 ===");
    let advanced_tests = vec![
        // 测试复杂条件查询
        "SELECT id, name, age FROM users WHERE id >= 2 AND age <= 30",
        "SELECT order_id, product FROM orders WHERE amount >= 100 AND amount <= 500",
        "SELECT name, stock FROM products WHERE stock > 0 AND price < 100",
        
        // 测试边界值
        "INSERT INTO products VALUES (5, 'Test Product', 0, 0)",
        "INSERT INTO products VALUES (6, 'Max Product', 2147483647, 2147483647)",
        "SELECT id, name, price, stock FROM products WHERE price = 0 OR price = 2147483647",
        
        // 测试多条件更新
        "UPDATE products SET price = 1000, stock = 20 WHERE id = 1",
        "UPDATE users SET age = 31, email = 'new@example.com' WHERE id = 2",
        
        // 测试多条件删除
        "DELETE FROM products WHERE stock = 0 OR price = 0",
        
        // 验证更新结果
        "SELECT id, name, price, stock FROM products",
        "SELECT id, name, age, email FROM users"
    ];

    // 第三组测试：错误处理测试
    println!("\n=== 错误处理测试 ===");
    let error_tests = vec![
        // 测试表已存在
        "CREATE TABLE users (id INT)",
        
        // 测试未知表
        "SELECT * FROM unknown_table",
        
        // 测试未知列
        "SELECT unknown_column FROM users",
        
        // 测试类型不匹配
        "INSERT INTO users VALUES ('not_a_number', 'name', 25, 'email')",
        
        // 测试非法值
        "INSERT INTO products VALUES (7, 'Invalid', -1, -100)",
        
        // 测试语法错误
        "SELEC * FROM users",
        "INSERT INTO",
        "DELETE FROM",
        
        // 测试约束违反
        "INSERT INTO users VALUES (1, NULL, 25, 'email')"  // name 是 NOT NULL
    ];

    // 执行所有测试
    for (i, command) in basic_tests.iter().chain(advanced_tests.iter()).chain(error_tests.iter()).enumerate() {
        println!("\n测试 #{}: {}", i + 1, command);
        match db.execute_sql(command) {
            Ok(_) => println!("执行成功"),
            Err(e) => println!("执行失败: {}", e),
        }
    }
} 