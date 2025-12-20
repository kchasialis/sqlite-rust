use anyhow::{Result, bail};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::io::prelude::*;

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

fn read_tbl_name(file: &mut File, cell_offset: u16) -> Result<String> {
    file.seek(SeekFrom::Start(cell_offset as u64))?;
    let mut varint_buffers = [0u8; 18];
    file.read_exact(&mut varint_buffers)?;

    let (rec_size, rec_size_bytes) = read_varint(&varint_buffers);
    let (_, rowid_bytes) = read_varint(&varint_buffers[rec_size_bytes..]);
    let total_bytes: u64 = (rec_size_bytes + rowid_bytes) as u64;

    let mut record_buffer = vec![0u8; rec_size as usize];
    let record_offset: u64 = cell_offset as u64 + total_bytes;
    file.seek(SeekFrom::Start(record_offset))?;
    (file).read_exact(&mut record_buffer)?;

    let (header_size, mut bytes_used) = read_varint(&record_buffer);
    let mut tbl_name_offset = 0;
    let mut i = 0;
    while i < 2 {
        let (serial_type_code, serial_bytes_used) = read_varint((&record_buffer[bytes_used..]));
        bytes_used += serial_bytes_used;
        tbl_name_offset += (serial_type_code - 13) / 2;
        i += 1;
    }
    let (tbl_name_size, _) = read_varint(&record_buffer[bytes_used..]);
    let start = header_size as usize + tbl_name_offset as usize;
    let end = start + ((tbl_name_size - 13) / 2) as usize;
    let tbl_name_bytes = &record_buffer[start..end];
    Ok(String::from_utf8_lossy(tbl_name_bytes).to_string())
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
        },
        ".tables" => {
            let mut file = File::open(&args[1])?;
            let mut header = [0; 100];
            file.read_exact(&mut header)?;

            let mut page_header = [0; 8];
            (file).read_exact(&mut page_header)?;
            let n_cells = u16::from_be_bytes([page_header[3], page_header[4]]);

            let n_bytes = (n_cells * 2) as usize;
            let mut cell_array_contents = vec![0u8; n_bytes];
            file.read_exact(&mut cell_array_contents)?;

            let mut i = 0;
            let mut results: Vec<String> = vec![];
            while i < n_bytes {
                let cell_offset = u16::from_be_bytes([cell_array_contents[i], cell_array_contents[i + 1]]);
                print!("{} ", read_tbl_name(& mut file, cell_offset)?);
                results.push(read_tbl_name(& mut file, cell_offset)?);
                i += 2
            }
            println!();
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
