use LogEvent;
use failure::Error;
use std::fs::File;
use std::io::{self, BufWriter};
use std::io::prelude::*;

pub trait EventOutput {
    fn output(&mut self, event: &LogEvent) -> Result<(), Error>;
}

pub struct StdOutput {
    output: io::Stdout,
}

impl StdOutput {
    pub fn new() -> Self {
        StdOutput { output: io::stdout() }
    }
}

impl EventOutput for StdOutput {
    fn output(&mut self, event: &LogEvent) -> Result<(), Error> {
        Ok(writeln!(self.output, "{:?}", event)?)
    }
}

pub struct FileOutput {
    output: BufWriter<File>,
}

impl FileOutput {
    pub fn new(filename: &str) -> Result<Self, Error> {
        Ok(FileOutput {
            output: BufWriter::new(File::create(filename)?),
        })
    }
}

impl EventOutput for FileOutput {
    fn output(&mut self, event: &LogEvent) -> Result<(), Error> {
        Ok(writeln!(self.output, "{:?}", event)?)
    }
}
