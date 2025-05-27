use simple_db::{execute_sql, run_simple_db};

fn main() {
    // 测试包含多条语句和注释的长SQL代码
    let long_sql = r#"
           CREATE TABLE books_test15 (
            id INT(32) PRIMARY KEY,
            name VARCHAR(100),
            left_num INT(32),
            discription VARCHAR(150),
            price INT NOT NULL
        );

        INSERT INTO books_test15 (id, name, discription, price)VALUES (1, "SETI", "Search for ET", 32);
        INSERT INTO books_test15 (left_num, id, name, price) VALUES (23, 2, "Rust Porgraming", 66);

        -- 查询表中的所有数据
        SELECT id, name, discription FROM books_test15 where left_num IS NULL and price < 50;
    "#;
    
    // 执行长SQL代码
    let mock_args = vec![
        "simple_db".to_string(),  // 程序名（通常是第一个参数）
        "./tests/input.txt".to_string()  // 文件路径参数
    ];
    let result = run_simple_db(mock_args);
    
    // // 显示执行结果
    // if result {
    //     println!("\n所有SQL语句执行成功！");
    // } else {
    //     println!("\n部分SQL语句执行失败，详见上方错误信息");
    // }
} 