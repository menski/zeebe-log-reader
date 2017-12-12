use std::{self, fmt, mem, iter, convert};

use failure::Error;
use decode::Decoder;

const CACHE_LINE_LENGTH: usize = 64;
const BLOCK_SIZE: usize = 4 * 1024;
const FRAME_ALIGNMENT: usize = 8;
const FRAME_MESSAGE: u16 = 0;
const FRAME_PADDING: u16 = 1;

#[repr(C, packed)]
struct FsLogSegment {
    id: u32,
    version: u16,
    _unused: u16,
    capacity: u32,
    _padding1: [u8; (2 * CACHE_LINE_LENGTH) - 4],
    size: u32,
    _padding2: [u8; (2 * CACHE_LINE_LENGTH) - 4],
}

impl fmt::Debug for FsLogSegment {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("FsLogSegment")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("capacity", &self.capacity)
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug)]
#[repr(C, packed)]
pub struct DataFrame {
    length: u32,
    version: u8,
    flags: u8,
    frame_type: u16,
    stream_id: u32,
}

#[repr(C, packed)]
pub struct LogEntry {
    version: u16,
    _reserved: u16,
    position: u64,
    raft_term: u32,
    producer: u32,
    source_event_stream_partition: u32,
    source_event_position: u64,
    key: u64,
    metadata_length: u16,
    _unused: u16,
}

impl fmt::Debug for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogEntry")
            .field("version", &self.version)
            .field("position", &self.position)
            .field("raft_term", &self.raft_term)
            .field("producer", &self.producer)
            .field("source_event_stream_partition", &self.source_event_stream_partition)
            .field("source_event_position", &self.source_event_position)
            .field("key", &self.key)
            .field("metadata_length", &self.metadata_length)
            .finish()
    }
}

#[derive(Debug)]
#[repr(C, packed)]
pub struct SbeHeader {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,
}


#[derive(Debug)]
#[repr(C, packed)]
pub struct Metadata {
    request_stream_id: i32,
    request_id: u64,
    subscription_id: u64,
    protcol_version: u16,
    event_type: u8,
    incident_key: u64,
}

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
    Unknown,
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
            _ => {
                eprintln!("Unknown event type: {}", event_type);
                EventType::Unknown
            }
        }
    }
}

#[derive(Debug)]
pub struct LogStream<'d> {
    decoder: Decoder<'d>,
}

impl<'d> LogStream<'d> {
    pub fn new(data: &'d [u8]) -> Self {
        let mut decoder = Decoder::new(data);
        let segment: &FsLogSegment = decoder.read_type().unwrap();
        decoder.truncate(segment.size as usize).unwrap();
        decoder.align(BLOCK_SIZE).unwrap();
        LogStream {
            decoder,
        }
    }

    fn next(&mut self) -> Result<Option<LogEvent>, Error> {
        let mut log_event = None;
        while !self.decoder.is_empty() && log_event.is_none() {
            let data_frame: &DataFrame = self.decoder.read_type()?;
            if data_frame.frame_type == FRAME_MESSAGE {
                let entry: &LogEntry = self.decoder.read_type()?;

                let sbe_header: &SbeHeader = self.decoder.read_type()?;
                assert_eq!(0, sbe_header.schema_id);
                assert_eq!(1, sbe_header.version);
                assert_eq!(200, sbe_header.template_id);

                let metadata: &Metadata = self.decoder.read_type()?;

                self.decoder.read(data_frame.length as usize - mem::size_of_val(data_frame) - mem::size_of_val(entry) - entry.metadata_length as usize)?;

                log_event = Some(LogEvent {
                    position: entry.position,
                    key: entry.key,
                    raft_term: entry.raft_term,
                    producer: entry.producer.into(),
                    source_event_position: if entry.source_event_position < std::u64::MAX { Some(entry.source_event_position)} else { None },
                    event_type: metadata.event_type.into(),
                });
            }

            self.decoder.align(FRAME_ALIGNMENT)?;
        }

        Ok(log_event)
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
    Unknown,
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
            _ => {
                eprintln!("Unknown event producer id: {}", id);
                Producer::Unknown
            }
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
    event_type: EventType
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

