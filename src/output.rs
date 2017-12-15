use failure::Error;
use std::fs::File;
use std::io::{self, BufWriter};
use std::io::prelude::*;
use data::Frame;
use EventType;
use msgpack::*;

pub trait EventOutput {
    fn output(&mut self, frame: &Frame) -> Result<(), Error>;
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
    fn output(&mut self, frame: &Frame) -> Result<(), Error> {
        let event_type: EventType = frame.entry.metadata.event_type.into();
        write!(self.output, "{{ position: {}, key: {}, source_event_position: {}, producer: {}, type: {:?} ",
                    frame.entry.log_entry.position,
                    frame.entry.log_entry.key,
                    frame.entry.log_entry.source_event_position,
                    frame.entry.log_entry.producer,
                    event_type)?;

        match event_type {
            EventType::Task => {
                let event: TaskEvent = deserialize(frame.entry.event)?;
                write!(self.output, "{:?}", event)?;
            }
            EventType::Workflow => {
                let event: WorkflowEvent = deserialize(frame.entry.event)?;
                write!(self.output, "{:?}", event)?;
            }
            EventType::WorkflowInstance => {
                let event: WorkflowInstanceEvent = deserialize(frame.entry.event)?;
                write!(self.output, "{:?}", event)?;
            }
            _ => {}
        }

        Ok(writeln!(self.output, " }}")?)
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
    fn output(&mut self, frame: &Frame) -> Result<(), Error> {
        let event_type: EventType = frame.entry.metadata.event_type.into();
        write!(self.output, "{{ position: {}, key: {}, source_event_position: {}, producer: {}, type: {:?} ",
               frame.entry.log_entry.position,
               frame.entry.log_entry.key,
               frame.entry.log_entry.source_event_position,
               frame.entry.log_entry.producer,
               event_type)?;

        match event_type {
            EventType::Task => {
                let event: TaskEvent = deserialize(frame.entry.event)?;
                write!(self.output, "{:?}", event)?;
            }
            EventType::Workflow => {
                let event: WorkflowEvent = deserialize(frame.entry.event)?;
                write!(self.output, "{:?}", event)?;
            }
            EventType::WorkflowInstance => {
                let event: WorkflowInstanceEvent = deserialize(frame.entry.event)?;
                write!(self.output, "{:?}", event)?;
            }
            _ => {}
        }

        Ok(writeln!(self.output, " }}")?)
    }
}
