use simple_db::execute_sql;

fn main() {
    // 测试包含多条语句和注释的长SQL代码
    let long_sql = r#"
    CREATE TABLE books_test14 (
    id INT(32) PRIMARY KEY,
    name VARCHAR(100),
    left_num INT(32),
    discription VARCHAR(150),
    price INT NOT NULL
);

INSERT INTO books_test14 (id, name, discription, price)VALUES (1, "SETI", "Search for ET", 32);
INSERT INTO books_test14 (left_num, id, name, price) VALUES (23, 2, "Rust Porgraming", 66);

-- 查询表中的所有数据
SELECT * FROM books_test14 where discription IS NOT NULL;
SELECT * FROM books_test14 where left_num IS NULL
    "#;
    
    // 执行长SQL代码
    let result = execute_sql(long_sql);
    
    // 显示执行结果
    if result {
        println!("\n所有SQL语句执行成功！");
    } else {
        println!("\n部分SQL语句执行失败，详见上方错误信息");
    }
} 