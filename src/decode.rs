use failure::Error;
use std::{fmt, mem};

fn align(value: usize, alignment: usize) -> usize {
    (value + (alignment - 1)) & !(alignment - 1)
}

pub struct Decoder<'d> {
    data: &'d [u8],
    position: usize,
}

impl<'d> fmt::Debug for Decoder<'d> {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Decoder")
            .field("position", &self.position)
            .field("capacity", &self.data.len())
            .finish()
    }
}

impl<'d> Decoder<'d> {
    pub fn new(data: &'d [u8]) -> Self {
        Decoder { data, position: 0 }
    }

    fn new_position(&self, bytes: usize) -> Result<usize, Error> {
        self.ensure_position(self.position + bytes)
    }

    fn ensure_position(&self, position: usize) -> Result<usize, Error> {
        if position < self.position {
            Err(format_err!(
                "Cannot move position to {} before current position {}",
                position,
                self.position
            ))

        } else if position <= self.data.len() {
            Ok(position)
        } else {
            let available = self.data.len() - self.position;
            let bytes = position - self.position;
            Err(format_err!(
                "Not enough bytes: only {} bytes left but {} required",
                available,
                bytes
            ))
        }
    }

    #[inline]
    pub fn read_type<T>(&mut self) -> Result<&'d T, Error> {
        let num_bytes = mem::size_of::<T>();
        let end = self.new_position(num_bytes)?;
        let slice = self.data[self.position..end].as_ptr() as *mut T;
        let value: &'d T = unsafe { &*slice };
        self.position = end;
        Ok(value)
    }

    #[inline]
    pub fn read(&mut self, bytes: usize) -> Result<&'d [u8], Error> {
        let end = self.new_position(bytes)?;
        let slice: &[u8] = &self.data[self.position..end];
        self.position = end;
        Ok(slice)
    }

    #[inline]
    pub fn truncate(&mut self, length: usize) -> Result<(), Error> {
        self.data = &self.data[0..self.ensure_position(length)?];
        Ok(())
    }

    #[inline]
    pub fn align(&mut self, alignment: usize) -> Result<(), Error> {
        let position = align(self.position, alignment);
        self.position = self.ensure_position(position)?;
        Ok(())
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.position >= self.data.len()
    }
}
