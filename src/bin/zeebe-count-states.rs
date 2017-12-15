extern crate failure;
extern crate zeebe_log_reader;

use failure::Error;

use std::collections::HashMap;
use std::fs::File;
use std::io::prelude::*;
use std::time::Instant;

use zeebe_log_reader::{EventType, LogStream};
use zeebe_log_reader::msgpack::*;

fn main() {
    let now = Instant::now();
    match try_main() {
        Ok(_) => println!("Took {:?}", now.elapsed()),
        Err(e) => eprintln!("Error: {}", e),
    }
}

fn try_main() -> Result<(), Error> {
    let mut workflow_instance_events = HashMap::new();
    let mut task_events = HashMap::new();

    for filename in std::env::args().skip(1) {
        let mut file = File::open(&filename)?;

        let mut buffer = Vec::new();
        file.read_to_end(&mut buffer)?;

        let logstream = LogStream::new(&buffer)?;

        for frame in logstream {
            match frame.entry.metadata.event_type.into() {
                EventType::Task => {
                    let e: TaskEvent = deserialize(frame.entry.event)?;
                    *task_events.entry(e.state).or_insert(0) += 1;
                },
                EventType::WorkflowInstance => {
                    let e: WorkflowInstanceEvent = deserialize(frame.entry.event)?;
                    *workflow_instance_events.entry(e.state).or_insert(0) += 1
                },
                _ => {}
            }
        }
    }

    for (state, count) in workflow_instance_events {
        println!("{:?}: {}", state, count);
    }

    for (state, count) in task_events {
        println!("{:?}: {}", state, count);
    }

    Ok(())
}
