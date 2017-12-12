// TODO: remove
#![allow(dead_code)]

#[macro_use]
extern crate failure;

mod decode;
mod logstream;

use std::fs::File;
use std::io::Read;

use logstream::LogStream;

fn main() {
    let filename = std::env::args().nth(1).unwrap_or("log/00.data".to_string());
    let mut file = File::open(filename).unwrap();

    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).unwrap();

    let logstream = LogStream::new(&buffer);

    for event in logstream {
        println!("{:?}", event);
    }
}
