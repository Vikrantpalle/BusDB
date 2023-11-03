#![allow(non_snake_case)]
#![feature(windows_by_handle)]

use std::sync::{Mutex, Arc};

use buffer::tuple::PageBuffer;
use storage::folder::Folder;

pub struct State {
    pub folder: Mutex<Arc<Folder>>,
    pub buf: Mutex<Arc<PageBuffer>>
}

pub mod storage;
pub mod buffer;
pub mod operator;
pub mod index;
pub mod compiler;
pub mod error;
