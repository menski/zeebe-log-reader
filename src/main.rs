#[macro_use]
extern crate failure;

mod decode;
mod logstream;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufWriter;

use logstream::LogStream;

fn main() {
    let mut output = BufWriter::new(File::create("log.txt").unwrap());
    std::env::args().skip(1).for_each(|filename| {
        let mut file = File::open(&filename).expect(&format!("Unable to open file {}", filename));

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer).unwrap();

        let logstream = LogStream::new(&buffer);

        for event in logstream {
            writeln!(output, "{:?}", event).unwrap();
        }
    });
}
