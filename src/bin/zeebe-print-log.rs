extern crate failure;
extern crate zeebe_log_reader;

use failure::Error;
use std::fs::File;
use std::io::BufWriter;
use std::io::prelude::*;
use std::time::Instant;

use zeebe_log_reader::LogStream;

const OUTPUT_FILE: &str = "log.txt";

fn main() {
    let now = Instant::now();
    match try_main() {
        Ok(_) => println!("Log written to file {} in {:?}", OUTPUT_FILE, now.elapsed()),
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn try_main() -> Result<(), Error> {
    let mut output = BufWriter::new(File::create(OUTPUT_FILE)?);

    for filename in std::env::args().skip(1) {
        let mut file = File::open(&filename)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let logstream = LogStream::new(&buffer)?;

        for event in logstream {
            writeln!(output, "{:?}", event)?;
        }
    }

    Ok(())
}
