pub struct TableFormatter;

impl TableFormatter {
    /// 格式化表格输出
    /// 所有字段在表单元格中，列中最长字段距离左右边界各1个空格，其他字段与最长字段向左对齐
    /// 每个单元格宽度至少为5个字符(包括内容和空格)，若超过则以列中最长内容+左右各1个空格为标准
    pub fn format_table(headers: &[String], rows: &[Vec<String>]) -> String {
        let mut result = String::new();
        
        // 计算每列的最大宽度
        let mut max_widths: Vec<usize> = vec![0; headers.len()];
        
        // 先检查表头宽度
        for (i, header) in headers.iter().enumerate() {
            max_widths[i] = max_widths[i].max(header.len());
        }
        
        // 再检查所有行的宽度
        for row in rows {
            for (i, cell) in row.iter().enumerate() {
                if i < max_widths.len() {
                    // 如果单元格是"NULL"则当作空字符串处理
                    let cell_width = if cell == "NULL" { 0 } else { cell.len() };
                    max_widths[i] = max_widths[i].max(cell_width);
                }
            }
        }
        
        // 确保最小宽度至少为3（这样加上左右各1个空格就是至少5个字符）
        for width in &mut max_widths {
            *width = (*width).max(3);
        }
        
        // 构建表头
        result.push_str(&Self::format_row(headers, &max_widths));
        result.push('\n');
        
        // 构建分隔线
        let mut separator = String::new();
        separator.push('|');
        
        for (i, width) in max_widths.iter().enumerate() {
            // 两边各留1个空格
            separator.push(' ');
            separator.push_str(&"-".repeat(*width));
            separator.push(' ');
            
            if i < max_widths.len() - 1 {
                separator.push('|');
            } else {
                separator.push('|');
            }
        }
        
        result.push_str(&separator);
        result.push('\n');
        
        // 构建数据行
        for row in rows {
            result.push_str(&Self::format_row(row, &max_widths));
            result.push('\n');
        }
        
        result
    }
    
    /// 格式化单行数据
    fn format_row(cells: &[String], widths: &[usize]) -> String {
        let mut row_line = String::new();
        row_line.push('|');
        
        for (i, cell) in cells.iter().enumerate() {
            if i < widths.len() {
                // 如果是"NULL"，则显示为空白
                let display_cell = if cell == "NULL" { "" } else { cell };
                
                // 计算需要的填充空格
                let padding = widths[i] - display_cell.len();
                
                // 确保左右各有一个空格，内容左对齐
                row_line.push(' ');
                row_line.push_str(display_cell);
                row_line.push_str(&" ".repeat(padding + 1)); // +1 确保右侧至少有一个空格
                
                if i < cells.len() - 1 {
                    row_line.push('|');
                } else {
                    row_line.push('|');
                }
            }
        }
        
        row_line
    }
} 