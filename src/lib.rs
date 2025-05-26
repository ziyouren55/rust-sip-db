pub mod core;
pub mod cli;

pub use core::db::{Database, StorageType};
use std::path::PathBuf;

/// 执行SQL语句的统一接口
/// 
/// # 参数
/// * `sql_statement` - 要执行的SQL语句
/// * `db_path` - 可选的数据库路径，如果不提供则使用内存存储
/// 
/// # 返回值
/// * `bool` - 执行成功返回true，失败返回false
pub fn execute_sql(sql_statement: &str, db_path: Option<PathBuf>) -> bool {
    // 创建数据库实例
    let storage_type = match db_path {
        Some(path) => StorageType::File(path),
        None => StorageType::Memory,
    };
    
    let mut db = Database::new(storage_type);
    
    // 执行SQL语句
    match db.execute_sql(sql_statement) {
        Ok(_) => true,
        Err(_) => false,
    }
}

/// 获取默认数据库路径
pub fn get_default_db_path() -> PathBuf {
    PathBuf::from(".")
}
