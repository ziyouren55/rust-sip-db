use simple_db::execute_sql;

fn main() {
    // 测试包含多条语句和注释的长SQL代码
    let long_sql = r#"
    -- 创建带有Float类型的表
CREATE TABLE test_float (
    id INT PRIMARY KEY,
    float_val FLOAT,
    int_val INT,
    name VARCHAR(50)
);

-- 插入测试数据
INSERT INTO test_float VALUES (1, 3.14, 10, 'test1');
INSERT INTO test_float VALUES (2, 2.5, 5, 'test2');
INSERT INTO test_float VALUES (3, 7.8, 15, 'test3');

-- 测试四则运算表达式
SELECT float_val + int_val, float_val - int_val, float_val * int_val, float_val / int_val FROM test_float;

-- 测试WHERE中的比较表达式
SELECT * FROM test_float WHERE float_val > 3.0;
SELECT * FROM test_float WHERE float_val + int_val > 10;
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