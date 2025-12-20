use anyhow::{Result, bail};
use std::fs::File;
use std::io::prelude::*;

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
        }
        _ => bail!("Missing or invalid command passed: {}", command),
    }

    Ok(())
}
