use std::fmt::format;
use anyhow::{Result, bail, Context};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use regex::Regex;

#[derive(Debug, Clone, Copy, PartialEq)]
enum SqlType {
    Integer,
    Text,
    Real,
    Blob,
    Null,
}

struct Column {
    name: String,
    tpe: SqlType,
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

fn extract_integer(buffer: &[u8], offset: usize, serial_type: u64) -> Result<i64> {
    match serial_type {
        0 => Ok(0),
        1 => {
            if offset >= buffer.len() {
                bail!("Offset {} out of bounds (buffer len: {})", offset, buffer.len());
            }
            Ok(buffer[offset] as i8 as i64)
        }
        2 => {
            let bytes: [u8; 2] = buffer[offset..offset + 2]
                .try_into()
                .context("Failed to read 2 bytes")?;
            Ok(i16::from_be_bytes(bytes) as i64)
        }
        3 => {
            if offset + 3 > buffer.len() {
                bail!("Not enough bytes for 24-bit integer");
            }
            let b1 = buffer[offset] as i32;
            let b2 = buffer[offset + 1] as i32;
            let b3 = buffer[offset + 2] as i32;

            let mut value = (b1 << 16) | (b2 << 8) | b3;

            if value & 0x800000 != 0 {
                value |= 0xFF000000u32 as i32;
            }

            Ok(value as i64)
        }
        4 => {
            let bytes: [u8; 4] = buffer[offset..offset + 4]
                .try_into()
                .context("Failed to read 4 bytes")?;
            Ok(i32::from_be_bytes(bytes) as i64)
        }
        5 => {
            if offset + 6 > buffer.len() {
                bail!("Not enough bytes for 48-bit integer");
            }

            let mut bytes = [0u8; 8];
            bytes[2..8].copy_from_slice(&buffer[offset..offset + 6]);

            let mut value = i64::from_be_bytes(bytes);

            if value & 0x800000000000 != 0 {
                value |= 0xFFFF000000000000u64 as i64;
            }

            Ok(value)
        }
        6 => {
            let bytes: [u8; 8] = buffer[offset..offset + 8]
                .try_into()
                .context("Failed to read 8 bytes")?;
            Ok(i64::from_be_bytes(bytes))
        }
        7 => {
            // 64-bit IEEE floating point (but you asked for integers)
            let bytes: [u8; 8] = buffer[offset..offset + 8]
                .try_into()
                .context("Failed to read 8 bytes for float")?;
            let float_val = f64::from_be_bytes(bytes);
            Ok(float_val as i64)  // Convert to integer (may lose precision)
        }
        8 => Ok(0),
        9 => Ok(1),
        _ => bail!("Serial type {} is not an integer type", serial_type),
    }
}

fn extract_real(buffer: &[u8], offset: usize, serial_type: u64) -> Result<f64> {
    match serial_type {
        7 => {
            let bytes: [u8; 8] = buffer[offset..offset + 8]
                .try_into()
                .context("Failed to read 8 bytes for float")?;
            let float_val = f64::from_be_bytes(bytes);
            Ok(float_val)
        }
        _ => bail!("Serial type {} is not an integer type", serial_type),
    }
}

fn extract_string(buffer: &[u8], offset: usize, serial_type: u64) -> String {
    let size = get_serial_type_size(serial_type);

    if size == 0 {
        return String::new();
    }

    if offset + size > buffer.len() {
        eprintln!("Warning: String exceeds buffer bounds");
        return String::new();
    }

    let bytes = &buffer[offset..offset + size];
    String::from_utf8_lossy(bytes).to_string()
}

fn parse_columns(sql_str: &str) -> Result<Vec<Column>> {
    let create_re = Regex::new(r"(?si)CREATE\s+TABLE\s+\w+\s*\((.*?)\)")?;

    if let Some(caps) = create_re.captures(sql_str) {
        let cols_section = &caps[1];

        let col_re = Regex::new(r"(\w+)\s+(\w+)[^,]*")?;

        let columns: Vec<Column> = col_re.captures_iter(cols_section)
            .map(|c| Column::from_strs(&c[1], &c[2]))
            .collect();

        return Ok(columns);
    }

    Ok(vec![])
}

fn get_cell_data(file: &mut File, page_offset: u64, cell_offset: u16) -> Result<(Vec<u64>, u64, Vec<u8>)> {
    let absolute_offset = page_offset + cell_offset as u64;

    file.seek(SeekFrom::Start(absolute_offset))?;
    let mut varint_buffers = [0u8; 18];
    file.read_exact(&mut varint_buffers)?;

    let (rec_size, rec_size_bytes) = read_varint(&varint_buffers);
    let (_, rowid_bytes) = read_varint(&varint_buffers[rec_size_bytes..]);
    let total_bytes = rec_size_bytes + rowid_bytes;

    file.seek(SeekFrom::Start(absolute_offset + total_bytes as u64))?;

    let mut record_buffer = vec![0u8; rec_size as usize];
    file.read_exact(&mut record_buffer)?;

    let (header_size, mut header_pos) = read_varint(&record_buffer);

    let mut serial_types = Vec::new();
    while header_pos < header_size as usize {
        let (serial_type, bytes) = read_varint(&record_buffer[header_pos..]);
        serial_types.push(serial_type);
        header_pos += bytes;
    }

    Ok((serial_types, header_size, record_buffer))
}

fn read_tbl_info(file: &mut File, cell_offset: u16) -> Result<TableInfo> {
    let (serial_types, header_size, record_buffer) = get_cell_data(file, 0, cell_offset)?;

    if serial_types.len() < 5 {
        bail!("Expected at least 5 columns in sqlite_schema, found {}", serial_types.len());
    }

    let mut body_offset = header_size as usize;

    // Column 0: type (text)
    let type_str = extract_string(&record_buffer, body_offset, serial_types[0]);
    body_offset += get_serial_type_size(serial_types[0]);

    // Column 1: name (text)
    let name_str = extract_string(&record_buffer, body_offset, serial_types[1]);
    body_offset += get_serial_type_size(serial_types[1]);

    // Column 2: tbl_name (text)
    let tbl_name_str = extract_string(&record_buffer, body_offset, serial_types[2]);
    body_offset += get_serial_type_size(serial_types[2]);

    // Column 3: rootpage (integer)
    let rootpage_int = extract_integer(&record_buffer, body_offset, serial_types[3])? as u32;
    body_offset += get_serial_type_size(serial_types[3]);

    // Column 4: sql (text)
    let sql_str = extract_string(&record_buffer, body_offset, serial_types[4]);

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

    // The page size is stored at the 16th byte offset, using 2 bytes in big-endian order
    #[allow(unused_variables)]
    let page_size: u16 = u16::from_be_bytes([header[16], header[17]]);

    let mut page_header = [0; 8];
    (file).read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

    println!("database page size: {}", page_size);
    println!("number of tables: {}", n_cells);

    Ok(())
}

fn get_tables_info(file: &mut File) -> Result<Vec<TableInfo>> {
    file.seek(SeekFrom::Start(100))?;

    let mut page_header = [0; 8];
    (file).read_exact(&mut page_header)?;
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

fn get_table_count(file: &mut File, rootpage: u32) -> Result<u64> {
    file.seek(SeekFrom::Start(0))?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let page_size: u16 = u16::from_be_bytes([header[16], header[17]]);
    let page_offset: u64 = (page_size as u32 * (rootpage - 1)) as u64;

    (file).seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 8];
    (file).read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as u64;

    Ok(n_cells)
}

fn get_col_data(file: &mut File, tinfo: &TableInfo, column_name: String) -> Result<Vec<String>> {
    let mut col_idx = 0;
    let mut column_type: SqlType = SqlType::Null;
    for (idx, col) in tinfo.columns.iter().enumerate() {
        if col.name.eq(&column_name) {
            column_type = col.tpe;
            col_idx = idx;
            break;
        }
    }

    file.seek(SeekFrom::Start(0))?;
    let mut header = [0; 100];
    file.read_exact(&mut header)?;

    let page_size: u16 = u16::from_be_bytes([header[16], header[17]]);
    let page_offset: u64 = (page_size as u32 * (tinfo.rootpage - 1)) as u64;

    (file).seek(SeekFrom::Start(page_offset))?;
    let mut page_header = [0; 8];
    (file).read_exact(&mut page_header)?;
    let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]) as u64;

    let n_bytes = (n_cells * 2) as usize;
    let mut cell_array_contents = vec![0u8; n_bytes];
    file.read_exact(&mut cell_array_contents)?;

    let mut results = Vec::new();
    for i in (0..n_bytes).step_by(2) {
        let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
        let (serial_types, header_size, record_buffer) = get_cell_data(file, page_offset, cell_offset)?;

        let mut body_offset = header_size as usize;
        for idx in 0..col_idx {
            body_offset += get_serial_type_size(serial_types[idx]);
        }

        let value = match column_type {
            SqlType::Integer => {
                extract_integer(&record_buffer, body_offset, serial_types[col_idx])?
                    .to_string()
            }
            SqlType::Text => {
                extract_string(&record_buffer, body_offset, serial_types[col_idx])
            }
            SqlType::Real => {
                extract_real(&record_buffer, body_offset, serial_types[col_idx])?
                    .to_string()
            }
            _ => bail!("Unsupported data type: {:?}", column_type)
        };

        results.push(value);
    }

    Ok(results)
}

fn execute_sql_query_command(args: &Vec<String>) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let tables_info = get_tables_info(&mut file)?;

    let count_regex = Regex::new(
        r"(?i)SELECT\s+COUNT\s*\(\s*\*\s*\)\s+FROM\s+(\w+)"
    )?;
    if let Some(caps) = count_regex.captures(&*args[2]) {
        let table_name = caps[1].to_string();
        for table in &tables_info {
            if table.tbl_name.eq(&table_name) {
                println!("{}", get_table_count(& mut file, table.rootpage)?);
                return Ok(());
            }
        }
    }

    let select_regex = Regex::new(
        r"(?i)SELECT\s+(.+?)\s+FROM\s+(\w+)"
    )?;
    if let Some(caps) = select_regex.captures(&*args[2]) {
        let cols_str = &caps[1];
        let table_name = caps[2].to_string();

        let col_names: Vec<String> = cols_str
            .split(',')
            .map(|s| s.trim().to_string())
            .collect();

        for tinfo in &tables_info {
            if tinfo.tbl_name.eq(&table_name) {
                let results: Vec<Vec<String>> = col_names
                    .iter()
                    .map(|col| get_col_data(&mut file, tinfo, col.clone()))
                    .collect::<Result<Vec<_>, _>>()?;

                let n_rows = results[0].len();
                for i in (0..n_rows) {
                    let row_vec: Vec<&str> = results.iter().map(|col | col[i].as_str()).collect();
                    println!("{}", row_vec.join("|"));
                }

                return Ok(());
            }
        }
    }

    bail!("Failed to find table name")
}

fn main() -> Result<()> {
    // Parse arguments
    let args = std::env::args().collect::<Vec<_>>();
    match args.len() {
        0 | 1 => bail!("Missing <database path> and <command>"),
        2 => bail!("Missing <command>"),
        _ => {}
    }

    // Parse command and act accordingly
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
