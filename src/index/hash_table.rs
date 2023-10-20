use std::io::{Write, Read, Error};

use crate::{storage::utils::{create_file, append_block, open_file}, buffer::{tuple::{HFILE_SUF, Tuple, TableIter, File, Schema}, ClockBuffer, Buffer}};
use serde::{Serialize, Deserialize};

const KEYNO: usize = 1 << 15;

#[derive(Serialize, Deserialize)]
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
        let Some(block_num) = self.table.keys[key as usize] else {self.block_num = None; return;};
        self.block_num = Some(block_num as u64);
        self.tup_idx = 0;
    }
}

pub trait Hash {
    fn read<'a>(self, key: u16) -> TableIter<HashTable>;
    fn insert(&mut self, p_buf: &mut ClockBuffer, key: u16, val: Tuple) -> Option<()>;
}

impl Hash for HashTable {

    fn read(self, key: u16) -> TableIter<HashTable> {
        let block_num = match self.keys[key as usize] {
            Some(v) => Some(v as u64),
            None => None
        };
        TableIter { 
            block_num, 
            tup_idx: 0, 
            table: self,  
            page: None,
            on_page_end: |i| {
                if !i.page.as_ref().unwrap().borrow().has_next() { return true;}
                i.block_num = Some(i.page.as_ref().unwrap().borrow().get_next().unwrap() as u64);
                i.tup_idx = 0;
                false 
            } 
        }
    }

    fn insert(&mut self, p_buf: &mut ClockBuffer, key: u16, val: Tuple) -> Option<()> {
        if self.keys[key as usize] == None {
            self.append_block().unwrap();
            self.keys[key as usize] = Some(self.num_blocks - 1);
            return self.insert(p_buf, key, val);
        }
        let block_num = self.keys[key as usize].unwrap();
        let mut page = p_buf.fetch((self.id as u64)<<32 | (block_num as u64 & 0xFFFFFFFF));
        let mut has_next = page.borrow().has_next();
        while has_next {
            let next = page.borrow().get_next().unwrap();
            page = p_buf.fetch((self.id as u64)<<32 | (next as u64 & 0xFFFFFFFF));
            has_next = page.borrow().has_next();
        }
        let mut p = page.borrow_mut();
        let bind = p.add(val.to_vec(), &self.schema);
        match bind {
            Some(_) => Some(()),
            None => {
                self.append_block().unwrap();
                p.set_next(self.num_blocks - 1);
                self.insert(p_buf, key, val)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use crate::buffer::{tuple::{DatumTypes, Datum, Operator}, ClockBuffer, Buffer};

    use super::{HashTable, Hash};

    #[test]
    pub fn test_hash_table() {
        let t_id = 6;
        HashTable::create(t_id, vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        let key = 10;
        let val = vec![Datum::Int(10), Datum::Int(20)];
        let val1 = vec![Datum::Int(10), Datum::Int(30)];
        let mut h = HashTable::new(t_id);
        let mut buf = ClockBuffer::new(10);
        h.insert(&mut buf, key, val.to_vec());
        h.insert(&mut buf, key+1, val1.to_vec());
        let ret: Vec<Vec<Datum>> = h.read(key).collect(&mut buf);
        assert_eq!(ret, vec![val]);
    }
}