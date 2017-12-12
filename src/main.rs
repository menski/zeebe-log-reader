#[macro_use]
extern crate failure;

use std::mem;
use failure::Error;
use std::fs::File;
use std::io::Read;

const CACHE_LINE_LENGTH: usize = 64;
const BLOCK_SIZE: usize = 4 * 1024;
const FRAME_ALIGNMENT: usize = 8;
const FRAME_MESSAGE: u16 = 0;
const FRAME_PADDING: u16 = 1;

fn align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}

#[repr(C)]
struct FsLogSegment {
    id: u32,
    version: u16,
    _unused: u16,
    capacity: u32,
    _padding1: [u8; (2 * CACHE_LINE_LENGTH) - 4],
    size: u32,
    _padding2: [u8; (2 * CACHE_LINE_LENGTH) - 4],
}

impl<'d> FsLogSegment {
    fn decode(mut decoder: Decoder<'d>) -> Result<(&'d Self, Decoder), Error> {
        let segment = decoder.read_type()?;

        // set decoder position after metadata
        decoder.position = align(decoder.position, BLOCK_SIZE);

        Ok((segment, decoder))
    }
}

impl std::fmt::Debug for FsLogSegment {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        f.debug_struct("FsLogSegment")
            .field("id", &self.id)
            .field("version", &self.version)
            .field("capacity", &self.capacity)
            .field("size", &self.size)
            .finish()
    }
}

#[derive(Debug)]
#[repr(C)]
struct DataFrame {
    length: u32,
    version: u8,
    flags: u8,
    frame_type: u16,
    stream_id: u32,
}

impl<'d> DataFrame {
    fn decode(mut decoder: Decoder<'d>) -> Result<(&'d Self, Decoder), Error> {
        let frame = decoder.read_type()?;

        Ok((frame, decoder))
    }

    fn is_message(&self) -> bool {
       self.frame_type == FRAME_MESSAGE
    }
}

#[derive(Debug)]
#[repr(C)]
struct LogEntry {
    version: u16,
    _reserved: u16,
    raft_term: u32,
    producer: u32,
    source_event_stream_partition: u32,
    source_event_position: u64,
    key: u64,
    metadata_length: u16,
    _unused: u16,
}

impl<'d> LogEntry {
    fn decode(mut decoder: Decoder<'d>) -> Result<(&'d Self, Decoder), Error> {
        let entry = decoder.read_type()?;

        Ok((entry, decoder))
    }
}

fn main() {
    let path = std::env::args().nth(1).unwrap_or("/home/menski/scratch/persistent-broker/logs/default-topic.1/00.data".to_string());

    let mut file = File::open(path).expect("Unable to open file");
    let mut buffer = Vec::new();
    file.read_to_end(&mut buffer).expect("Unable to read file");

    let decoder = Decoder::new(&buffer);
    let (segment, decoder) = FsLogSegment::decode(decoder).expect("Unable to read fs log segment header");
    let (frame, decoder) = DataFrame::decode(decoder).expect("Unable to read data frame");
    let (entry, decoder) = LogEntry::decode(decoder).expect("Unable to read log entry");

    println!("{:?} ", segment);
    println!("{:?} ", frame);
    println!("{:?} ", entry);
}

#[derive(Debug)]
struct Decoder<'d> {
    data: &'d [u8],
    position: usize,
}

impl<'d> Decoder<'d> {
    fn new(data: &'d [u8]) -> Self {
        Decoder { data, position: 0 }
    }

    #[inline]
    fn read_type<T>(&mut self) -> Result<&'d T, Error> {
        let num_bytes = mem::size_of::<T>();
        let end = self.position + num_bytes;
        if end <= self.data.len() {
            let slice = self.data[self.position..end].as_ptr() as *mut T;
            let value: &'d T = unsafe { &*slice };
            self.position = end;
            Ok(value)
        } else {
            let available = self.data.len() - self.position;
            Err(format_err!(
                "Not enough bytes: only {} bytes left but {} required",
                available,
                num_bytes
            ))
        }
    }

    #[inline]
    fn truncate(&mut self, length: usize) -> Result<(), Error> {
        if length < self.position {
            Err(format_err!(
                "Unable to truncate to {} before position {}",
                length,
                self.position
            ))
        } else if length <= self.data.len() {
            self.data = &self.data[0..length];
            Ok(())
        } else {
            Err(format_err!(
                "Unable to truncate to {} with smaller length {}",
                length,
                self.data.len()
            ))
        }

    }

    #[inline]
    fn skip(&mut self, bytes: usize) -> Result<(), Error> {
        let position = self.position + bytes;
        if position <= self.data.len() {
            self.position = position;
            Ok(())
        } else {
            let available = self.data.len() - self.position;

            Err(format_err!(
                "Not enough bytes: only {} bytes left but {} required",
                available,
                bytes
            ))
        }
    }

    #[inline]
    fn remaining(self) -> &'d [u8] {
        &self.data[self.position..]
    }
}
