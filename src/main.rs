#[macro_use]
extern crate failure;

mod data;
mod decode;
mod logstream;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;

use failure::Error;

use logstream::LogStream;

const OUTPUT_FILE: &str = "log.txt";

fn main() {
    match try_main() {
        Ok(_) => println!("Log written to file: {}", OUTPUT_FILE),
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
