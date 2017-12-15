#[macro_use]
extern crate failure;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_bytes;
extern crate rmp_serde;

mod data;
mod decode;
pub mod msgpack;
pub mod output;

use data::*;
use decode::Decoder;

use failure::Error;
use std::{convert, iter, mem};

#[derive(Debug)]
pub enum EventType {
    Task,
    Raft,
    Subscription,
    Subscriber,
    Deployment,
    WorkflowInstance,
    Incident,
    Workflow,
    Noop,
    Topic,
    Partition,
    Unknown(u8),
}

impl<'d> convert::From<u8> for EventType {
    fn from(event_type :u8) -> Self {
        match event_type {
            0 => EventType::Task,
            1 => EventType::Raft,
            2 => EventType::Subscription,
            3 => EventType::Subscriber,
            4 => EventType::Deployment,
            5 => EventType::WorkflowInstance,
            6 => EventType::Incident,
            7 => EventType::Workflow,
            8 => EventType::Noop,
            9 => EventType::Topic,
            10 => EventType::Partition,
            _ => EventType::Unknown(event_type),
        }
    }
}

#[derive(Debug)]
pub struct LogStream<'d> {
    decoder: Decoder<'d>,
    filter: Option<u8>,
}

impl<'d> LogStream<'d> {
    pub fn new(data: &'d [u8]) -> Result<Self, Error> {
        let mut decoder = Decoder::new(data);
        decode_fs_log_segment(&mut decoder)?;
        Ok(LogStream {
            decoder,
            filter: None,
        })
    }

    pub fn event_filter(&mut self, filter: u8) {
        self.filter = Some(filter);
    }

    fn next(&mut self) -> Result<Option<Frame<'d>>, Error> {
        while !self.decoder.is_empty() {
            let data_frame: &DataFrame = self.decoder.read_type()?;
            let entry = match data_frame.frame_type {
                FRAME_MESSAGE => {
                    let log_entry: &LogEntry = self.decoder.read_type()?;

                    let sbe_header: &SbeHeader = self.decoder.read_type()?;
                    assert_eq!(&Metadata::sbe_header(), sbe_header);

                    let metadata: &Metadata = self.decoder.read_type()?;

                    let event = self.decoder.read(
                        data_frame.length as usize - mem::size_of_val(data_frame) - mem::size_of_val(log_entry) -
                            log_entry.metadata_length as usize,
                    )?;

                    Some(Entry {
                        log_entry,
                        metadata,
                        event,
                    })
                }
                _ => None,
            };

            self.decoder.align(FRAME_ALIGNMENT)?;

            if let Some(entry) = entry {

                return Ok(Some(Frame { data_frame, entry }));
            }
        }

        Ok(None)
    }
}

#[derive(Debug)]
enum Producer {
    TaskQueue,
    TaskLock,
    TaskExpireLock,
    TopicSubscriptionPush,
    TopicSubscriptionManagement,
    Deployment,
    WorkflowInstance,
    Incident,
    SystemCreateTopic,
    SystemCollectPartition,
    Unknown(u32),
    None,
}

impl convert::From<u32> for Producer {
    fn from(id: u32) -> Self {
        match id {
            10 => Producer::TaskQueue,
            20 => Producer::TaskLock,
            30 => Producer::TaskExpireLock,
            40 => Producer::TopicSubscriptionPush,
            50 => Producer::TopicSubscriptionManagement,
            60 => Producer::Deployment,
            70 => Producer::WorkflowInstance,
            80 => Producer::Incident,
            1000 => Producer::SystemCreateTopic,
            1001 => Producer::SystemCollectPartition,
            std::u32::MAX => Producer::None,
            _ => Producer::Unknown(id),
        }
    }
}

#[derive(Debug)]
pub struct LogEvent {
    position: u64,
    key: u64,
    raft_term: u32,
    producer: Producer,
    source_event_position: Option<u64>,
    pub event_type: EventType,
}

impl<'d> iter::Iterator for LogStream<'d> {
    type Item = Frame<'d>;

    fn next(&mut self) -> Option<Self::Item> {
        match self.next() {
            Ok(o) => o,
            Err(e) => {
                eprintln!("Unable to get next entry: {}", e);
                None
            }
        }
    }
}
