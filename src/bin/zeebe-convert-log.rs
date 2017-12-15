#[macro_use]
extern crate failure;
extern crate zeebe_log_reader;
extern crate structopt;
#[macro_use]
extern crate structopt_derive;

use failure::Error;
use std::fs::File;
use std::io::prelude::*;
use std::time::Instant;
use std::convert;

use structopt::StructOpt;

use zeebe_log_reader::{LogStream, EventType};
use zeebe_log_reader::output::*;

#[derive(StructOpt, Debug)]
#[structopt(name = "zeebe-convert-log", about = "Convert Zeebe log streams to plain text output")]
struct Opt {
    #[structopt(short = "c", long = "console", help = "Output to console instead of file")]
    console: bool,
    #[structopt(help = "Input files")]
    input: Vec<String>,
    #[structopt(short = "f", long = "filter", help = "Filter event type")]
    filter: Option<EventType>,
}

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
    let opt = Opt::from_args();

    let mut output: Box<EventOutput> = Box::new(StdOutput::new());

    for filename in opt.input.iter() {
        let now = Instant::now();
        let mut file = File::open(&filename)?;

        if !opt.console {
            let output_name = format!("{}.{}", filename, "txt");
            print!("Converting {} to {}", filename, output_name);

            output = Box::new(FileOutput::new(&output_name)?);
        }


        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let logstream = LogStream::new(&buffer)?;

        for event in logstream {
            if let Some(event_type) = opt.filter {
            }
            output.output(&event)?;
        }

        if !opt.console {
            let duration = now.elapsed();
            println!(". Took {}.{:09}s", duration.as_secs(), duration.subsec_nanos());
        }
    }

    Ok(())
}
