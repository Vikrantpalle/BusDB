use std::{io::{Write, Read}, sync::Arc, ptr};

use crate::{storage::utils::{create_file, append_block, open_file}, buffer::{tuple::{HFILE_SUF, Tuple, TableIter, File, Schema, ClockBuffer}, Buff}, error::Error};
use serde::{Serialize, Deserialize};

const KEYNO: usize = 1 << 15;

#[derive(Serialize, Deserialize, Debug)]
pub struct HashTable {
    id: u32,
    num_blocks: u32,
    pub keys: Vec<Option<u32>>,
    schema: Schema
}

impl File for HashTable {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_schema(&self) -> Schema {
        self.schema.to_vec()
    }
}

impl HashTable {

    pub fn create(id: u32, schema: Schema) {
        create_file(&id.to_string()).unwrap();
        let mut h_file = create_file(&(id.to_string() + HFILE_SUF)).expect("could not create header file");
        let table = HashTable {
            id,
            num_blocks: 0,
            keys: vec![None; KEYNO],
            schema
        };
        h_file.write_all(&bincode::serialize(&table).unwrap()).unwrap();
    }

    pub fn new(id: u32) -> Self {
        let mut h_file = open_file(&(id.to_string() + HFILE_SUF)).expect("could not open header file");
        let mut bytes = Vec::new();
        h_file.read_to_end(&mut bytes).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }

    pub fn append_block(&mut self) -> Result<(), Error> {
        append_block(&self.id.to_string()).unwrap();
        self.num_blocks += 1;
        Ok(())
    }
}

pub trait HashIter {
    fn swap_key(&mut self, key: u16);
}

impl HashIter for TableIter<HashTable> {
    fn swap_key(&mut self, key: u16) {
        self.block_num = self.table.keys[key as usize].map(|v| v as u64);
        self.tup_idx = 0;
    }
}

pub trait Hash {
    fn read<'a>(self, key: u16, buf: Arc<ClockBuffer>) -> TableIter<HashTable>;
    fn insert(&mut self, key: u16, val: Tuple, buf: Arc<ClockBuffer>) -> Result<(), Error>;
}

impl Hash for HashTable {

    fn read<'a>(self, key: u16, buf: Arc<ClockBuffer>) -> TableIter<HashTable> {
        let block_num = self.keys[key as usize].map(|v| v as u64);
        let buf = Arc::clone(&buf);
        TableIter { 
            block_num, 
            buf,
            tup_idx: 0, 
            table: self,  
            page: ptr::null(),
            on_page_end: |i| {
                let page = unsafe { i.page.as_ref().unwrap().read().unwrap() };
                if !page.has_next() { return true;}
                i.block_num = Some(page.get_next().unwrap() as u64);
                drop(page);
                i.tup_idx = 0;
                false 
            } 
        }
    }

    fn insert(&mut self, key: u16, val: Tuple, buf: Arc<ClockBuffer>) -> Result<(), Error> {
        if self.keys[key as usize] == None {
            self.append_block().unwrap();
            self.keys[key as usize] = Some(self.num_blocks - 1);
            return self.insert(key, val, Arc::clone(&buf))
        }
        let block_num = self.keys[key as usize].unwrap();
        let mut page = buf.fetch((self.id as u64)<<32 | (block_num as u64 & 0xFFFFFFFF));
        let mut next;
        {
            let page_read = page.read().unwrap();
            next = page_read.get_next();
            drop(page_read);
        }
        while next.is_some() {
            page = buf.fetch((self.id as u64)<<32 | (next.unwrap() as u64 & 0xFFFFFFFF));
            {
                let page_read = page.read().unwrap();
                next = page_read.get_next();
                drop(page_read);
            }
        }
        let mut p = page.write().unwrap();
        let bind = p.add(val.to_vec(), &self.schema);
        match bind {
            Ok(_) => Ok(()),
            Err(_) => {
                self.append_block().unwrap();
                p.set_next(self.num_blocks - 1);
                drop(p);
                self.insert(key, val, Arc::clone(&buf))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::buffer::{tuple::{DatumTypes, Datum}, Buffer};

    use super::{HashTable, Hash};

    #[test]
    pub fn test_hash_table() {
        let t_id = 6;
        HashTable::create(t_id, vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        let key = 10;
        let val = vec![Datum::Int(10), Datum::Int(20)];
        let val1 = vec![Datum::Int(10), Datum::Int(30)];
        let mut h = HashTable::new(t_id);
        let buf = Arc::new(Buffer::new(10));
        h.insert(key, val.to_vec(), Arc::clone(&buf)).unwrap();
        h.insert(key+1, val1.to_vec(), Arc::clone(&buf)).unwrap();
        let ret: Vec<Vec<Datum>> = h.read(key, Arc::clone(&buf)).collect();
        assert_eq!(ret, vec![val]);
    }
}