use std::env;
use simple_db::run_simple_db;

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // 收集命令行参数
    let args: Vec<String> = env::args().collect();
    
    // 调用库函数处理命令行参数并运行程序
    run_simple_db(args)
}

//cargo run ./target/release/simple_db ./tests/input.txt
