extern crate failure;
extern crate zeebe_log_reader;

use failure::Error;
use std::fs::File;
use std::io::BufWriter;
use std::io::prelude::*;
use std::time::Instant;

use zeebe_log_reader::LogStream;

fn main() {
    let now = Instant::now();
    match try_main() {
        Ok(_) => {
            let duration = now.elapsed();
            println!("Took {}.{:09}s", duration.as_secs(), duration.subsec_nanos())
        },
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn try_main() -> Result<(), Error> {

    for filename in std::env::args().skip(1) {
        let now = Instant::now();

        let output_name = format!("{}.{}", filename, "txt");
        print!("Converting {} to {}", filename, output_name);

        let mut file = File::open(&filename)?;
        let mut output = BufWriter::new(File::create(output_name)?);

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let logstream = LogStream::new(&buffer)?;

        for event in logstream {
            writeln!(output, "{:?}", event)?;
        }

        let duration = now.elapsed();
        println!(". Took {}.{:09}s", duration.as_secs(), duration.subsec_nanos());
    }

    Ok(())
}
