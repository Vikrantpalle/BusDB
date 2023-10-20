use std::{fs::{OpenOptions, File, remove_file}, io::Write};

use super::{BASE_PATH, Block, DATSIZ};

pub fn create_file(file_name: &str) -> Result<File, std::io::Error> {
    File::create(BASE_PATH.to_owned()+"/"+file_name)
}

pub fn open_file(file_name: &str) -> Result<File, std::io::Error> {
    File::open(BASE_PATH.to_owned()+"/"+file_name)
}

pub fn write_file(file_name: &str) -> Result<File, std::io::Error> {
    OpenOptions::new().write(true).open(BASE_PATH.to_owned()+"/"+file_name)
}

pub fn delete_file(file_name: &str) -> Result<(), std::io::Error> {
    remove_file(BASE_PATH.to_owned()+"/"+file_name)
}

pub fn append_block(file_name: &str) -> Result<(), std::io::Error> {
    let mut f = OpenOptions::new().append(true).open(BASE_PATH.to_owned() + "/" + file_name).expect("Could not open file to append block");
    let new_block = Block {
        block_id: 0,
        next: 0,
        flags: 0,
        lower: 0,
        upper: DATSIZ as u16,
        data: [0; DATSIZ]
    };
    let buf = bincode::serialize(&new_block).expect("Could not serialize block");
    f.write_all(buf.as_slice())
}