use std::{self, convert, iter};

use failure::Error;
use decode::Decoder;

use data::*;

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

impl convert::From<u8> for EventType {
    fn from(event_type: u8) -> Self {
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
                        }) = frame.entry
            {
                return Ok(Some(LogEvent {
                    position: log_entry.position,
                    key: log_entry.key,
                    raft_term: log_entry.raft_term,
                    producer: log_entry.producer.into(),
                    source_event_position: log_entry.source_event_position(),
                    event_type: metadata.event_type.into(),
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
    event_type: EventType,
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
