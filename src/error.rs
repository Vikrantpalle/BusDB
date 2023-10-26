use std::{fmt, io::Error as IoError};

#[derive(Debug)]
pub enum PageError {
    NoBlock,
    OutOfBounds,
    InvalidTuple
}

#[derive(Debug)]
pub enum Error {
    Io(IoError),
    PageError(PageError),
    ParseError,
    InvalidName
}

impl From<IoError> for Error {
    fn from(value: IoError) -> Self {
        Self::Io(value)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Error")
    }
}