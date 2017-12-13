#[macro_use]
extern crate failure;
extern crate serde;
#[macro_use]
extern crate serde_derive;
extern crate serde_bytes;
extern crate rmp_serde;

mod data;
mod decode;
mod msgpack;

use data::*;
use decode::Decoder;

use failure::Error;
use msgpack::*;
use std::{convert, iter};

#[derive(Debug)]
pub enum EventType {
    Task(TaskEvent),
    Raft,
    Subscription,
    Subscriber,
    Deployment,
    WorkflowInstance(WorkflowInstanceEvent),
    Incident,
    Workflow(WorkflowEvent),
    Noop,
    Topic,
    Partition,
    Unknown(u8),
}

impl<'d> convert::From<(u8, &'d [u8])> for EventType {
    fn from((event_type, event): (u8, &'d [u8])) -> Self {
        match event_type {
            0 => EventType::Task(deserialize(event).unwrap()),
            1 => EventType::Raft,
            2 => EventType::Subscription,
            3 => EventType::Subscriber,
            4 => EventType::Deployment,
            5 => EventType::WorkflowInstance(deserialize(event).unwrap()),
            6 => EventType::Incident,
            7 => EventType::Workflow(deserialize(event).unwrap()),
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
}

impl<'d> LogStream<'d> {
    pub fn new(data: &'d [u8]) -> Result<Self, Error> {
        let mut decoder = Decoder::new(data);
        decode_fs_log_segment(&mut decoder)?;
        Ok(LogStream { decoder })
    }

    fn next(&mut self) -> Result<Option<LogEvent>, Error> {
        while !self.decoder.is_empty() {
            let frame = decode_frame(&mut self.decoder)?;
            if let Some(Entry {
                            log_entry,
                            metadata,
                            event,
                        }) = frame.entry
            {
                return Ok(Some(LogEvent {
                    position: log_entry.position,
                    key: log_entry.key,
                    raft_term: log_entry.raft_term,
                    producer: log_entry.producer.into(),
                    source_event_position: log_entry.source_event_position(),
                    event_type: (metadata.event_type, event).into(),
                }));

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
    type Item = LogEvent;

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
