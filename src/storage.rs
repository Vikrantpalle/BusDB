#![allow(dead_code)]

use serde::{Serialize, Deserialize};
use serde_with::{serde_as, Bytes};

pub const BLCKSIZ: usize = 8 * 1024;
pub const DATSIZ: usize = 8171;
pub const LOCSIZ: u16 = 2;
pub const BASE_PATH: &str = "C:/Users/vikra/rustDB/cache";

#[derive(Debug, Clone, Copy)]
pub enum Flags {
    Dirty = 1,
    Next = 2
}

pub mod disk_manager;
pub mod utils;
pub mod folder;


#[serde_as]
#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct Block {
    pub block_id: u32,
    pub next: u32,
    pub flags: u8,
    pub lower: u16, 
    pub upper: u16,
    #[serde_as(as = "Bytes")]
    pub data: [u8; DATSIZ]
}

impl Block {
    pub fn new(id: u32) -> Self {
        Block { block_id: id, next: 0, flags: 0, lower: 0, upper: DATSIZ as u16, data: [0; DATSIZ] }
    }

    #[inline]
    pub fn set_flag(&mut self, flag: &Flags) {
        self.flags |= *flag as u8
    }

    #[inline]
    pub fn toggle_flag(&mut self, flag: &Flags) {
        self.flags ^= *flag as u8
    }

    #[inline]
    pub fn check_flag(&self, flag: &Flags) -> bool {
        (self.flags & *flag as u8) == *flag as u8
    }

    #[inline]
    pub fn get_next(&self) -> u32 {
        self.next
    }

    #[inline]
    pub fn set_next(&mut self, next: u32) {
        self.set_flag(&Flags::Next);
        self.next = next;
    }
}

#[cfg(test)]
mod tests {
    use crate::storage::BLCKSIZ;

    use super::Block;

    #[test]
    fn test_block_size() {
        let block = Block::new(0);
        let size = bincode::serialize(&block).unwrap().len();
        assert_eq!(size, BLCKSIZ)
    } 
}
