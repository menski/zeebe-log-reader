use std::{self, fmt, mem};
use failure::Error;
use decode::Decoder;

const CACHE_LINE_LENGTH: usize = 64;
const BLOCK_SIZE: usize = 4 * 1024;
const FRAME_ALIGNMENT: usize = 8;
const FRAME_MESSAGE: u16 = 0;

#[repr(C, packed)]
pub struct FsLogSegment {
    pub id: u32,
    pub version: u16,
    _unused: u16,
    pub capacity: u32,
    _padding1: [u8; (2 * CACHE_LINE_LENGTH) - 4],
    pub size: u32,
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
    pub length: u32,
    pub version: u8,
    pub flags: u8,
    pub frame_type: u16,
    pub stream_id: u32,
}

#[repr(C, packed)]
pub struct LogEntry {
    pub version: u16,
    _reserved: u16,
    pub position: u64,
    pub raft_term: u32,
    pub producer: u32,
    pub source_event_stream_partition: u32,
    pub source_event_position: u64,
    pub key: u64,
    pub metadata_length: u16,
    _unused: u16,
}

impl fmt::Debug for LogEntry {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("LogEntry")
            .field("version", &self.version)
            .field("position", &self.position)
            .field("raft_term", &self.raft_term)
            .field("producer", &self.producer)
            .field(
                "source_event_stream_partition",
                &self.source_event_stream_partition,
            )
            .field("source_event_position", &self.source_event_position)
            .field("key", &self.key)
            .field("metadata_length", &self.metadata_length)
            .finish()
    }
}

impl LogEntry {
    pub fn source_event_position(&self) -> Option<u64> {
       if self.source_event_position < std::u64::MAX {
           Some(self.source_event_position)
       } else {
           None
       }
    }
}

#[derive(Debug, PartialEq)]
#[repr(C, packed)]
pub struct SbeHeader {
    pub block_length: u16,
    pub template_id: u16,
    pub schema_id: u16,
    pub version: u16,
}


#[derive(Debug)]
#[repr(C, packed)]
pub struct Metadata {
    pub request_stream_id: i32,
    pub request_id: u64,
    pub subscription_id: u64,
    pub protcol_version: u16,
    pub event_type: u8,
    pub incident_key: u64,
}

impl Metadata {
    pub fn sbe_header() -> SbeHeader {
        SbeHeader {
            block_length: mem::size_of::<Metadata>() as u16,
            template_id: 200,
            schema_id: 0,
            version: 1,
        }
    }
}

pub struct Entry<'d> {
    pub log_entry: &'d LogEntry,
    pub metadata: &'d Metadata,
}

pub struct Frame<'d> {
    pub data_frame: &'d DataFrame,
    pub entry: Option<Entry<'d>>,
}

pub fn decode_fs_log_segment<'d>(decoder: &'d mut Decoder) -> Result<&'d FsLogSegment, Error> {
    let segment: &FsLogSegment = decoder.read_type()?;
    decoder.truncate(segment.size as usize)?;
    decoder.align(BLOCK_SIZE)?;
    Ok(segment)
}

pub fn decode_frame<'d>(decoder: &'d mut Decoder) -> Result<Frame<'d>, Error> {
    let data_frame: &DataFrame = decoder.read_type()?;
    let entry = match data_frame.frame_type {
        FRAME_MESSAGE => {
            let log_entry: &LogEntry = decoder.read_type()?;

            let sbe_header: &SbeHeader = decoder.read_type()?;
            assert_eq!(&Metadata::sbe_header(), sbe_header);

            let metadata: &Metadata = decoder.read_type()?;

            // TODO: capture buffer
            decoder.read(
                data_frame.length as usize - mem::size_of_val(data_frame) - mem::size_of_val(log_entry) -
                    log_entry.metadata_length as usize,
            )?;

            Some(Entry {
                log_entry,
                metadata,
            })
        },
        _ => None
    };

    decoder.align(FRAME_ALIGNMENT)?;

    Ok(Frame {
        data_frame,
        entry
    })
}
