use std::io::{SeekFrom, Read, Seek, Write};

use super::{BLCKSIZ, utils::{open_file, write_file}, Block};

pub const SET_64: u64 = 0xFFFFFFFFFFFFFFFF; 

pub fn write_block(page_id: u128, block: &Block) -> Option<()> {
    let f_id = (page_id>>64 as u64).to_string();
    let b_id = (page_id&SET_64 as u128) as u64;
    let mut f = write_file(&f_id).expect("Could not write to file");
    let bytes = bincode::serialize(block).expect("Could not serialize block");
    f.seek(SeekFrom::Start(b_id * BLCKSIZ as u64)).unwrap();
    f.write_all(&bytes).expect("Could not read page from file");
    Some(())
}

pub fn read_block(page_id: u128) -> Block {
    let f_id = (page_id>>64 as u64).to_string();
    let b_id = (page_id&SET_64 as u128) as u64;
    let mut f = open_file(&f_id).expect("File not found");
    let mut block = [0; BLCKSIZ];
    f.seek(SeekFrom::Start(b_id * BLCKSIZ as u64)).unwrap();
    f.read(&mut block).expect("Could not read page from file");
    bincode::deserialize(&block).unwrap()
}