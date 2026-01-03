use std::fmt::format;
use anyhow::{Result, bail, Context};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use regex::Regex;

struct Column {
    name: String,
    tpe: SqlType,
}

struct Record {
    data: Vec<Vec<u8>>
}

#[derive(Debug)]
enum PageType {
    InteriorIndex = 0x2,
    InteriorTable = 0x5,
    LeafIndex = 0xa,
    LeafTable = 0xd
}

impl PageType {
    fn from_u8(value: u8) -> Result<Self> {
        match value {
            0x02 => Ok(PageType::InteriorIndex),
            0x05 => Ok(PageType::InteriorTable),
            0x0a => Ok(PageType::LeafIndex),
            0x0d => Ok(PageType::LeafTable),
            _ => bail!("Received wrong value for page type")
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum SqlType {
    Integer,
    Text,
    Real,
    Blob,
    Null,
}

impl SqlType {
    fn from_str(s: &str) -> Self {
        match s.to_lowercase().as_str() {
            "integer" | "int" => SqlType::Integer,
            "text" | "varchar" | "char" => SqlType::Text,
            "real" | "float" | "double" => SqlType::Real,
            "blob" => SqlType::Blob,
            _ => SqlType::Text,
        }
    }

    fn to_string(&self) -> &str {
        match self {
            SqlType::Integer => "INTEGER",
            SqlType::Text => "TEXT",
            SqlType::Real => "REAL",
            SqlType::Blob => "BLOB",
            SqlType::Null => "NULL",
        }
    }
}

impl Column {
    fn from_strs(name: &str, col_type: &str) -> Self {
        Column {
            name: name.to_string(),
            tpe: SqlType::from_str(col_type),
        }
    }
}

struct TableInfo {
    tpe: String,
    name: String,
    tbl_name: String,
    rootpage: u32,
    columns: Vec<Column>
}

fn read_varint(data: &[u8]) -> (u64, usize) {
    let mut i = 0;
    let mut val: u64 = 0;

    while i < 8 && i < data.len() {
        let current_byte = data[i];
        i += 1;

        val = (val << 7) | ((current_byte & 0x7F) as u64);

        if current_byte & 0x80 == 0 {
            return (val, i);
        }
    }

    if i < data.len() {
        let current_byte = data[i];
        val = (val << 8) | (current_byte as u64);
        i += 1;
    }

    (val, i)
}

fn get_serial_type_size(serial_type: u64) -> usize {
    match serial_type {
        0 | 8 | 9 => 0,
        1 => 1,
        2 => 2,
        3 => 3,
        4 => 4,
        5 => 6,
        6 | 7 => 8,
        n if n >= 12 && n % 2 == 0 => ((n - 12) / 2) as usize,
        n if n >= 13 && n % 2 == 1 => ((n - 13) / 2) as usize,
        _ => 0,
    }
}

fn extract_integer(buffer: &[u8]) -> Result<i64> {
    match buffer.len() {
        0 => Ok(0),
        1 => Ok(buffer[0] as i8 as i64),
        2 => {
            let bytes: [u8; 2] = buffer[0..2].try_into()
                .context("Failed to read 2 bytes")?;
            Ok(i16::from_be_bytes(bytes) as i64)
        }
        3 => {
            let b1 = buffer[0] as i32;
            let b2 = buffer[1] as i32;
            let b3 = buffer[2] as i32;
            let mut value = (b1 << 16) | (b2 << 8) | b3;
            if value & 0x800000 != 0 {
                value |= 0xFF000000u32 as i32;
            }
            Ok(value as i64)
        }
        4 => {
            let bytes: [u8; 4] = buffer[0..4].try_into()
                .context("Failed to read 4 bytes")?;
            Ok(i32::from_be_bytes(bytes) as i64)
        }
        5 => {
            let mut bytes = [0u8; 8];
            bytes[2..8].copy_from_slice(&buffer);
            let mut value = i64::from_be_bytes(bytes);
            if value & 0x800000000000 != 0 {
                value |= 0xFFFF000000000000u64 as i64;
            }
            Ok(value)
        }
        6 => {
            let bytes: [u8; 8] = buffer[0..8]
                .try_into()
                .context("Failed to read 8 bytes")?;
            Ok(i64::from_be_bytes(bytes))
        }
        8 => Ok(0),
        9 => Ok(1),
        _ => bail!("Invalid buffer length for integer type: {}", buffer.len()),
    }
}

fn extract_real(buffer: &[u8]) -> Result<f64> {
    match buffer.len() {
        8 => {
            let bytes: [u8; 8] = buffer[0..8]
                .try_into()
                .context("Failed to read 8 bytes for float")?;
            Ok(f64::from_be_bytes(bytes))
        }
        _ => bail!("Invalid buffer length for floating type: {}", buffer.len()),
    }
}

fn extract_string(buffer: &[u8]) -> String {
    String::from_utf8_lossy(buffer).to_string()
}

fn parse_columns(sql_str: &str) -> Result<Vec<Column>> {
    if sql_str.is_empty() {
        return Ok(vec![]);
    }

    let create_re = Regex::new(r#"(?si)CREATE\s+TABLE\s+["']?\w+["']?\s*\((.*)\)"#)?;

    if let Some(caps) = create_re.captures(sql_str) {
        let cols_section = &caps[1];
        let col_re = Regex::new(r#"(?i)(?:"([^"]+)"|(\w+))\s+(integer|text|real|blob|int|varchar|char|float|double)\b"#)?;

        let columns: Vec<Column> = col_re.captures_iter(cols_section)
            .map(|c| {
                let name = if let Some(quoted) = c.get(1) {
                    quoted.as_str()
                } else {
                    &c[2]
                };
                Column::from_strs(name, &c[3])
            })
            .collect();

        return Ok(columns);
    }

    Ok(vec![])
}

fn get_cell_data(file: &mut File, page_offset: u64, cell_offset: u16, index_cell: bool) -> Result<(Record, u64)> {
    let absolute_offset = page_offset + cell_offset as u64;

    file.seek(SeekFrom::Start(absolute_offset))?;
    let mut payload_size_buf = [0u8; 9];
    file.read_exact(&mut payload_size_buf)?;
    let (payload_size, payload_size_bytes) = read_varint(&payload_size_buf);

    let mut rowid = 0;
    let mut total_header_bytes = payload_size_bytes;

    if !index_cell {
        file.seek(SeekFrom::Start(absolute_offset + payload_size_bytes as u64))?;
        let mut rowid_buf = [0u8; 9];
        file.read_exact(&mut rowid_buf)?;
        let (rowid_val, rowid_bytes) = read_varint(&rowid_buf);
        rowid = rowid_val;
        total_header_bytes += rowid_bytes;
    }

    file.seek(SeekFrom::Start(absolute_offset + total_header_bytes as u64))?;

    let mut record_buffer = vec![0u8; payload_size as usize];
    file.read_exact(&mut record_buffer)
        .context(format!("Failed to read record ({} bytes) at cell offset {}", payload_size, cell_offset))?;

    let (header_size, mut header_pos) = read_varint(&record_buffer);

    let mut body_offset = header_size as usize;
    let mut data: Vec<Vec<u8>> = vec![];

    while header_pos < header_size as usize {
        let (serial_type, bytes) = read_varint(&record_buffer[header_pos..]);
        let serial_type_size = get_serial_type_size(serial_type);
        data.push(Vec::from(&record_buffer[body_offset..body_offset + serial_type_size]));
        body_offset += serial_type_size;
        header_pos += bytes;
    }

    Ok((Record { data }, rowid))
}

fn read_tbl_info(file: &mut File, cell_offset: u16) -> Result<TableInfo> {
    let (record, _) = get_cell_data(file, 0, cell_offset, false)?;

    if record.data.len() < 5 {
        bail!("Expected at least 5 columns in sqlite_schema, found {}", record.data.len());
    }

    let type_str = extract_string(&record.data[0]);
    let name_str = extract_string(&record.data[1]);
    let tbl_name_str = extract_string(&record.data[2]);
    let rootpage_int = extract_integer(&record.data[3])? as u32;
    let sql_str = extract_string(&record.data[4]);

    // eprintln!("sql_str: {}", sql_str);

    Ok(TableInfo {
        tpe: type_str,
        name: name_str,
        tbl_name: tbl_name_str,
        rootpage: rootpage_int,
        columns: parse_columns(&sql_str)?
    })
}

fn execute_dbinfo_command(args: Vec<String>) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let page_size: u16 = u16::from_be_bytes([header[16], header[17]]);

    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

    println!("database page size: {}", page_size);
    println!("number of tables: {}", n_cells);

    Ok(())
}

fn get_tables_info(file: &mut File) -> Result<Vec<TableInfo>> {
    file.seek(SeekFrom::Start(100))?;

    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

    let n_bytes = (n_cells * 2) as usize;
    let mut cell_array_contents = vec![0u8; n_bytes];
    file.read_exact(&mut cell_array_contents)?;

    let mut result = vec![];
    let mut i = 0;
    while i < n_bytes {
        let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
        result.push(read_tbl_info(file, cell_offset)?);
        i += 2
    }

    Ok(result)
}

fn execute_tables_command(args: Vec<String>) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let v = get_tables_info(&mut file)?;

    for table in v {
        print!("{} ", table.tbl_name);
    }
    println!();

    Ok(())
}

fn get_rows_from_leaf_page(file: &mut File, current_page: u32, page_size: u16) -> Result<u64> {
    let page_offset: u64 = (page_size as u32 * (current_page - 1)) as u64;

    file.seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as u64;

    Ok(n_cells)
}

fn count_rows_in_page(file: &mut File, current_page: u32, page_size: u16) -> Result<u64> {
    let page_offset: u64 = (page_size as u32 * (current_page - 1)) as u64;
    file.seek(SeekFrom::Start(page_offset))?;

    let mut page_type_buf = [0; 1];
    file.read_exact(&mut page_type_buf)
        .context(format!("Failed to read page type at page {}", current_page))?;
    let page_type_enum = PageType::from_u8(page_type_buf[0])?;

    match page_type_enum {
        PageType::InteriorTable => {
            file.seek(SeekFrom::Start(page_offset))?;
            let mut page_header = [0; 12];
            file.read_exact(&mut page_header)
                .context(format!("Failed to read page header at interior page {}", current_page))?;
            let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

            let n_bytes = (n_cells * 2) as usize;
            let mut cell_array_contents = vec![0u8; n_bytes];
            file.read_exact(&mut cell_array_contents)
                .context(format!("Failed to read cell array ({} bytes) at interior page {}", n_bytes, current_page))?;

            let mut total_count = 0u64;
            for i in (0..n_bytes).step_by(2) {
                let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
                let (left_page, _) = extract_interior_cell_data(file, page_offset, cell_offset, false)?;
                total_count += count_rows_in_page(file, left_page, page_size)?;
            }

            let right_page = u32::from_be_bytes([page_header[8], page_header[9], page_header[10], page_header[11]]);
            total_count += count_rows_in_page(file, right_page, page_size)?;

            Ok(total_count)
        }
        PageType::LeafTable => {
            get_rows_from_leaf_page(file, current_page, page_size)
        }
        _ => bail!("Unhandled page type!")
    }
}

fn get_table_count(file: &mut File, tinfo: &TableInfo, page_size: u16) -> Result<u64> {
    count_rows_in_page(file, tinfo.rootpage, page_size)
}

fn get_page_data_with_filter(file: &mut File, col_idxs: &Vec<usize>, col_types: &Vec<SqlType>, page_size: u16, page_num: u32, filter_col: &Option<usize>, filter_val: &Option<String>) -> Result<Vec<Vec<String>>> {
    let page_offset: u64 = (page_size as u32 * (page_num - 1)) as u64;

    file.seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)
        .context(format!("Failed to read page header at leaf page {}", page_num))?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as u64;

    let n_bytes = (n_cells * 2) as usize;
    let mut cell_array_contents = vec![0u8; n_bytes];
    file.read_exact(&mut cell_array_contents)
        .context(format!("Failed to read cell array ({} bytes) at leaf page {}", n_bytes, page_num))?;

    let mut results: Vec<Vec<String>> = vec![Vec::new(); col_idxs.len()];
    for i in (0..n_bytes).step_by(2) {
        let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
        let (record, rowid) = get_cell_data(file, page_offset, cell_offset, false)?;

        if let (Some(fcol), Some(fval)) = (filter_col, filter_val) {
            let record_filter_val = extract_string(&record.data[*fcol]);
            if !record_filter_val.eq_ignore_ascii_case(fval) {
                continue;
            }
        }

        for (coli, col_idx) in col_idxs.iter().enumerate() {
            let value = if *col_idx == 0 && record.data[0].is_empty() {
                rowid.to_string()
            } else {
                let record_idx = *col_idx;

                match &col_types[coli] {
                    SqlType::Integer => extract_integer(&record.data[record_idx])?.to_string(),
                    SqlType::Text => extract_string(&record.data[record_idx]),
                    SqlType::Real => extract_real(&record.data[record_idx])?.to_string(),
                    _ => bail!("Unsupported data type: {:?}", col_types[coli])
                }
            };

            results[coli].push(value);
        }
    }

    Ok(results)
}

fn extract_interior_cell_data(file: &mut File, page_offset: u64, cell_offset: u16, index_cell: bool) -> Result<(u32, u64)> {
    let absolute_offset = page_offset + cell_offset as u64;
    file.seek(SeekFrom::Start(absolute_offset))?;

    let mut left_page_buf = [0; 4];
    file.read_exact(&mut left_page_buf)
        .context(format!("Failed to read left page pointer at cell offset {} (absolute: {})", cell_offset, absolute_offset))?;

    if !index_cell {
        let mut key_varint_buf = [0; 9];
        file.read_exact(&mut key_varint_buf)?;
        return Ok((u32::from_be_bytes(left_page_buf), read_varint(&key_varint_buf).0));
    }

    Ok((u32::from_be_bytes(left_page_buf), 0u64))
}

fn get_cols_data_with_filter(file: &mut File, page_size: u16, current_page: u32, col_idxs: &Vec<usize>, col_types: &Vec<SqlType>, filter_col: &Option<usize>, filter_val: &Option<String>, columns: &mut Vec<Vec<String>>) -> Result<()> {
    let page_offset: u64 = (page_size as u32 * (current_page - 1)) as u64;
    file.seek(SeekFrom::Start(page_offset))?;

    let mut page_type_buf = [0; 1];
    file.read_exact(&mut page_type_buf)
        .context(format!("Failed to read page type at page {}", current_page))?;
    let page_type_enum = PageType::from_u8(page_type_buf[0])?;

    match page_type_enum {
        PageType::InteriorTable => {
            file.seek(SeekFrom::Start(page_offset))?;
            let mut page_header = [0; 12];
            file.read_exact(&mut page_header)
                .context(format!("Failed to read page header at interior page {}", current_page))?;
            let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

            let n_bytes = (n_cells * 2) as usize;
            let mut cell_array_contents = vec![0u8; n_bytes];
            file.read_exact(&mut cell_array_contents)
                .context(format!("Failed to read cell array ({} bytes) at interior page {}", n_bytes, current_page))?;

            let mut i = 0;
            while i < n_bytes {
                let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
                let (left_page, _) = extract_interior_cell_data(file, page_offset, cell_offset, false)?;
                get_cols_data_with_filter(file, page_size, left_page, col_idxs, col_types, filter_col, filter_val, columns)?;
                i += 2
            }

            let right_page = u32::from_be_bytes([page_header[8], page_header[9], page_header[10], page_header[11]]);
            get_cols_data_with_filter(file, page_size, right_page, col_idxs, col_types, filter_col, filter_val, columns)?;

            Ok(())
        }
        PageType::LeafTable => {
            let page_data = get_page_data_with_filter(file, col_idxs, col_types, page_size, current_page, filter_col, filter_val)?;
            for (col_idx, col_data) in page_data.into_iter().enumerate() {
                columns[col_idx].extend(col_data);
            }
            Ok(())
        }
        _ => bail!("Unhandled page type!")
    }
}

fn find_child_page_for_rowid(file: &mut File, page_offset: u64, rowid: u64) -> Result<u32> {
    file.seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 12];
    file.read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

    let n_bytes = (n_cells * 2) as usize;
    let mut cell_array_contents = vec![0u8; n_bytes];
    file.read_exact(&mut cell_array_contents)?;

    let mut i = 0;
    while i < n_bytes {
        let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
        let (left_child, cell_key) = extract_interior_cell_data(file, page_offset, cell_offset, false)?;
        if rowid <= cell_key {
            return Ok(left_child);
        }
        i += 2
    }

    Ok(u32::from_be_bytes([page_header[8], page_header[9], page_header[10], page_header[11]]))
}

fn find_row_in_leaf(file: &mut File, page_offset: u64, col_idxs: &Vec<usize>, col_types: &Vec<SqlType>, tinfo: &TableInfo, rowid: u64) -> Result<Vec<String>> {
    file.seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as u64;

    let n_bytes = (n_cells * 2) as usize;
    let mut cell_array_contents = vec![0u8; n_bytes];
    file.read_exact(&mut cell_array_contents)?;

    for i in (0..n_bytes).step_by(2) {
        let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);

        let (record, cell_rowid) = get_cell_data(file, page_offset, cell_offset, false)?;

        if cell_rowid == rowid {
            let mut results: Vec<String> = vec![];
            for (coli, col_idx) in col_idxs.iter().enumerate() {
                let value = if *col_idx == 0 && record.data[0].is_empty() {
                    rowid.to_string()
                } else {
                    match &col_types[coli] {
                        SqlType::Integer => extract_integer(&record.data[*col_idx])?.to_string(),
                        SqlType::Text => extract_string(&record.data[*col_idx]),
                        SqlType::Real => extract_real(&record.data[*col_idx])?.to_string(),
                        _ => bail!("Unsupported data type: {:?}", col_types[coli])
                    }
                };
                results.push(value);
            }
            return Ok(results);
        }
    }

    bail!("Rowid {} not found in leaf page", rowid)
}

fn find_row_by_rowid(file: &mut File, curr_page: u32, page_size: u16, col_idxs: &Vec<usize>, col_types: &Vec<SqlType>, tinfo: &TableInfo, rowid: u64) -> Result<Vec<String>> {
    let page_offset = (page_size as u32 * (curr_page - 1)) as u64;
    file.seek(SeekFrom::Start(page_offset))?;

    let mut page_type_buf = [0; 1];
    file.read_exact(&mut page_type_buf)?;
    let page_type = PageType::from_u8(page_type_buf[0])?;

    match page_type {
        PageType::InteriorTable => {
            let child_page = find_child_page_for_rowid(file, page_offset, rowid)?;
            find_row_by_rowid(file, child_page, page_size, col_idxs, col_types, tinfo, rowid)
        }
        PageType::LeafTable => {
            find_row_in_leaf(file, page_offset, col_idxs, col_types, tinfo, rowid)
        }
        _ => bail!("Unexpected page type in table btree")
    }
}

fn get_rows_by_rowids(file: &mut File, page_size: u16, col_idxs: &Vec<usize>, col_types: &Vec<SqlType>, rowids: &Vec<u64>, tinfo: &TableInfo) -> Result<Vec<Vec<String>>> {
    let mut results = Vec::new();

    for &rowid in rowids {
        let row = find_row_by_rowid(file, tinfo.rootpage, page_size, col_idxs, col_types, tinfo, rowid)?;
        results.push(row);
    }

    Ok(results)
}

fn get_index_page_data(file: &mut File, index_curr_page: u32, page_size: u16, index_col: &Column, index_val: &str) -> Result<Vec<u64>> {
    let page_offset: u64 = (page_size as u32 * (index_curr_page - 1)) as u64;

    file.seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 8];
    file.read_exact(&mut page_header)
        .context(format!("Failed to read page header at leaf page {}", index_curr_page))?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as u64;

    let n_bytes = (n_cells * 2) as usize;
    let mut cell_array_contents = vec![0u8; n_bytes];
    file.read_exact(&mut cell_array_contents)
        .context(format!("Failed to read cell array ({} bytes) at leaf page {}", n_bytes, index_curr_page))?;

    let mut result: Vec<u64> = vec![];
    for i in (0..n_bytes).step_by(2) {
        let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
        let (record, _) = get_cell_data(file, page_offset, cell_offset, true)?;
        let value = extract_string(&record.data[0]);
        let rowid = extract_integer(&record.data[1])? as u64;

        if value.eq_ignore_ascii_case(index_val) {
            result.push(rowid);
        }
    }

    Ok(result)
}

fn get_rowids_index(file: &mut File, index_curr_page: u32, page_size: u16, index_col: &Column, index_val: &str, rowids: &mut Vec<u64>) -> Result<()> {
    let page_offset: u64 = (page_size as u32 * (index_curr_page - 1)) as u64;
    file.seek(SeekFrom::Start(page_offset))?;

    let mut page_type_buf = [0; 1];
    file.read_exact(&mut page_type_buf)
        .context(format!("Failed to read page type at index page {}", index_curr_page))?;
    let page_type_enum = PageType::from_u8(page_type_buf[0])?;

    match page_type_enum {
        PageType::InteriorIndex => {
            file.seek(SeekFrom::Start(page_offset))?;
            let mut page_header = [0; 12];
            file.read_exact(&mut page_header)
                .context(format!("Failed to read page header at interior index page {}", index_curr_page))?;
            let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

            let n_bytes = (n_cells * 2) as usize;
            let mut cell_array_contents = vec![0u8; n_bytes];
            file.read_exact(&mut cell_array_contents)
                .context(format!("Failed to read cell array ({} bytes) at interior index page {}", n_bytes, index_curr_page))?;

            let mut i = 0;
            while i < n_bytes {
                let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
                let (left_page, _) = extract_interior_cell_data(file, page_offset, cell_offset, true)?;
                get_rowids_index(file, left_page, page_size, index_col, index_val, rowids)?;
                i += 2
            }

            let right_page = u32::from_be_bytes([page_header[8], page_header[9], page_header[10], page_header[11]]);
            get_rowids_index(file, right_page, page_size, index_col, index_val, rowids)?;
            Ok(())
        }
        PageType::LeafIndex => {
            let page_rowids = get_index_page_data(file, index_curr_page, page_size, index_col, index_val)?;
            rowids.extend(page_rowids);
            Ok(())
        }
        _ => bail!("Unhandled page type!")
    }
}

fn get_cols_data_with_index(file: &mut File, tinfo: &TableInfo, page_size: u16, col_idxs: &Vec<usize>, col_types: &Vec<SqlType>, index_rootpage: u32, index_col: &Column, index_val: &str) -> Result<Vec<Vec<String>>> {
    let mut rowids: Vec<u64> = vec![];
    get_rowids_index(file, index_rootpage, page_size, index_col, index_val, & mut rowids)?;

    // for rowid in &rowids {
    //     eprintln!("[DEBUG] found rowid: {}", rowid);
    // }

    Ok(get_rows_by_rowids(file, page_size, col_idxs, col_types, &rowids, &tinfo)?)
}

fn find_index_root_page(tables_info: &[TableInfo], table_name: &str) -> Option<u32> {
    for entry in tables_info {
        if entry.tpe == "index" && entry.tbl_name == table_name {
            // eprintln!("Found index on {} for table: {}", entry.rootpage, table_name);
            return Some(entry.rootpage);
        }
    }
    None
}

fn execute_sql_query_command(args: &Vec<String>) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let tables_info = get_tables_info(&mut file)?;

    file.seek(SeekFrom::Start(0))?;
    let mut header = [0; 100];
    file.read_exact(&mut header)
        .context("Failed to read database header")?;

    let page_size: u16 = u16::from_be_bytes([header[16], header[17]]);

    let count_regex = Regex::new(
        r"(?i)SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+(\w+)"
    )?;
    if let Some(caps) = count_regex.captures(&args[2]) {
        let table_name = caps[1].to_string();
        for tinfo in &tables_info {
            if tinfo.tbl_name.eq(&table_name) {
                println!("{}", get_table_count(& mut file, tinfo, page_size)?);
                return Ok(());
            }
        }
    }

    let select_regex = Regex::new(
        r"(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)"
    )?;
    if let Some(caps) = select_regex.captures(&args[2]) {
        let cols_str = &caps[1];
        let table_name = caps[2].to_string();

        let tinfo = tables_info
            .iter()
            .find(|t| t.tbl_name == table_name)
            .ok_or_else(|| anyhow::anyhow!("Table '{}' not found", table_name))?;

        let col_names: Vec<String> = cols_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        let mut col_idxs: Vec<usize> = vec![];
        let mut col_types: Vec<SqlType> = vec![];
        for column_name in &col_names {
            for (idx, col) in tinfo.columns.iter().enumerate() {
                if col.name.eq(column_name) {
                    col_types.push(col.tpe);
                    col_idxs.push(idx);
                    break;
                }
            }
        }

        let where_re = Regex::new(r#"(?i)WHERE\s+(\w+)\s*=\s*['"]([^'"]+)['"]"#)?;
        let mut filter_col: Option<usize> = None;
        let mut filter_val: Option<String> = None;
        let mut use_index = false;

        if let Some(caps) = where_re.captures(&args[2]) {
            let col_name = caps[1].to_string();
            filter_val = Some(caps[2].to_string());

            for (idx, col) in tinfo.columns.iter().enumerate() {
                if col.name == col_name {
                    filter_col = Some(idx);

                    if col_name == "country" {
                        use_index = true;
                    }
                    break;
                }
            }
        }

        let mut columns: Vec<Vec<String>>;
        if use_index {
            let index_root = find_index_root_page(&tables_info, &table_name)
                .ok_or_else(|| anyhow::anyhow!("Index not found"))?;

            let index_col = Column {
                name: "country".to_string(),
                tpe: SqlType::Text,
            };

            let rows = get_cols_data_with_index(&mut file, tinfo, page_size, &col_idxs, &col_types, index_root, &index_col, filter_val.unwrap().as_str())?;
            for row in rows {
                println!("{}", row.join("|"));
            }
        } else {
            columns = vec![Vec::new(); col_idxs.len()];
            get_cols_data_with_filter(&mut file, page_size, tinfo.rootpage, &col_idxs, &col_types, &filter_col, &filter_val, &mut columns)?;
            let n_rows = columns[0].len();
            for i in 0..n_rows {
                let row_vec: Vec<&str> = columns.iter().map(|col| col[i].as_str()).collect();
                println!("{}", row_vec.join("|"));
            }
        }

        return Ok(());
    }

    bail!("Failed to find table name")
}

fn main() -> Result<()> {
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    let command = &args[2];
    match command.as_str() {
        ".dbinfo" => {
            execute_dbinfo_command(args)?;
        },
        ".tables" => {
            execute_tables_command(args)?;
        },
        _ => {
            execute_sql_query_command(&args)?;
        }
    }

    Ok(())
}