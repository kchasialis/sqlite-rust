use anyhow::{Result, bail, Context};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};

struct TableInfo {
    tpe: String,
    name: String,
    tbl_name: String,
    rootpage: u32,
    sql: String
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

fn parse_rootpage(buffer: &[u8], offset: usize, serial_type: u64) -> Result<u32> {
    match serial_type {
        0 | 8 => Ok(0),
        9 => Ok(1),
        1 => {
            if offset >= buffer.len() {
                bail!("Offset {} out of bounds (buffer len: {})", offset, buffer.len());
            }
            Ok(buffer[offset] as u32)
        }
        2 => {
            let bytes: [u8; 2] = buffer[offset..offset + 2]
                .try_into()
                .context("Failed to read 2 bytes for rootpage")?;
            Ok(u16::from_be_bytes(bytes) as u32)
        }
        4 => {
            let bytes: [u8; 4] = buffer[offset..offset + 4]
                .try_into()
                .context("Failed to read 4 bytes for rootpage")?;
            Ok(u32::from_be_bytes(bytes))
        }
        _ => bail!("Unexpected serial type {} for rootpage", serial_type),
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

fn read_tbl_info(file: &mut File, cell_offset: u16) -> Result<TableInfo> {
    file.seek(SeekFrom::Start(cell_offset as u64))?;
    let mut varint_buffers = [0u8; 18];
    file.read_exact(&mut varint_buffers)?;

    let (rec_size, rec_size_bytes) = read_varint(&varint_buffers);
    let (_, rowid_bytes) = read_varint(&varint_buffers[rec_size_bytes..]);
    let total_bytes = rec_size_bytes + rowid_bytes;

    file.seek(SeekFrom::Start(cell_offset as u64 + total_bytes as u64))?;

    let mut record_buffer = vec![0u8; rec_size as usize];
    file.read_exact(&mut record_buffer)?;

    let (header_size, mut header_pos) = read_varint(&record_buffer);

    let mut serial_types = Vec::new();
    while header_pos < header_size as usize {
        let (serial_type, bytes) = read_varint(&record_buffer[header_pos..]);
        serial_types.push(serial_type);
        header_pos += bytes;
    }

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
    let rootpage_int = parse_rootpage(&record_buffer, body_offset, serial_types[3])?;
    body_offset += get_serial_type_size(serial_types[3]);

    // Column 4: sql (text)
    let sql_str = extract_string(&record_buffer, body_offset, serial_types[4]);

    Ok(TableInfo {
        tpe: type_str,
        name: name_str,
        tbl_name: tbl_name_str,
        rootpage: rootpage_int,
        sql: sql_str,
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

fn parse_table_name_from_command(command: &str) -> Result<String> {
    let parts: Vec<&str> = command.split_whitespace().collect();

    if parts.len() < 4 {
        bail!("Invalid query format");
    }

    if parts[0].to_uppercase() != "SELECT" {
        bail!("Query must start with SELECT");
    }

    if parts[2].to_uppercase() != "FROM" {
        bail!("Expected FROM keyword");
    }

    Ok(parts[3].to_string())
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

fn execute_sql_query_command(args: &Vec<String>) -> Result<()> {
    let mut file = File::open(&args[1])?;
    let table_name = parse_table_name_from_command(&*args[2])?;
    let table_names = get_tables_info(&mut file)?;

    for table in table_names {
        if table.tbl_name.eq(&table_name) {
            println!("{}", get_table_count(& mut file, table.rootpage)?);
            return Ok(());
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
