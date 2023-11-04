use std::{sync::Arc, ptr};

use crate::{storage::{utils::{append_block, delete_file}, folder::{Folder, TableInode}, disk_manager::SET_64}, buffer::{tuple::{Tuple, TableIter, Table, Schema, PageBuffer}, Buff}, error::Error};
use serde::{Serialize, Deserialize};

const KEYNO: usize = 1 << 15;

#[derive(Serialize, Deserialize, Debug)]
pub struct HashTable {
    inode: TableInode,
    temp: bool,
    num_blocks: u32,
    pub keys: Vec<Option<u32>>,
    schema: Schema
}

impl Drop for HashTable {
    fn drop(&mut self) { 
        if self.temp() { 
            delete_file(&self.inode.data_ino.to_string()).unwrap();
        }
    }
}

impl Table for HashTable {
    fn inode(&self) -> TableInode {
        self.inode.clone()
    }

    fn set_inode(&mut self, inode: TableInode) {
        self.inode = inode
    }

    fn temp(&self) -> bool {
        self.temp
    }

    fn set_temp(&mut self, temp: bool) {
        self.temp = temp
    }

    fn schema(&self) -> Schema {
        self.schema.to_vec()
    }

    fn set_schema(&mut self, schema: Schema) {
        self.schema = schema
    }

    fn create(f: Arc<Folder>, name: &str, schema: Schema) -> Result<Self, Error> {
        Ok(f.create_table(name, schema)?)
    }

    fn create_temp(f: Arc<Folder>, schema: Schema) -> Result<Self, Error> {
        Ok(f.create_temp_table(schema)?)
    }

    fn new(f: Arc<Folder>, name: &str) -> Result<Self, Error> {
        Ok((f.fetch_table(&name)?).ok_or(Error::TableDoesNotExist)?)
    }
}

impl Default for HashTable {
    fn default() -> Self {
        HashTable {
            inode: TableInode::new(0, 0),
            temp: false,
            num_blocks: 0,
            keys: vec![None; KEYNO],
            schema: vec![]
        }
    }
}

impl HashTable {

    pub fn append_block(&mut self) -> Result<(), Error> {
        append_block(&self.inode.data_ino.to_string()).unwrap();
        self.num_blocks += 1;
        Ok(())
    }

}

pub trait HashIter {
    fn swap_key(&mut self, key: u16);
}

impl TableIter<HashTable> {
    pub fn new(buf: Arc<PageBuffer>, table: HashTable) -> Self {
        TableIter { block_num: None, buf: Arc::clone(&buf), tup_idx: 0, table, page: ptr::null_mut(), on_page_end: |i| {
            let page = unsafe { i.page.as_ref().unwrap().read().unwrap() };
            if !page.has_next() { return true;}
            i.block_num = Some(page.get_next().unwrap() as u64);
            drop(page);
            i.tup_idx = 0;
            false 
            }
        }
    }
}

impl HashIter for TableIter<HashTable> {
    fn swap_key(&mut self, key: u16) {
        self.block_num = self.table.keys[key as usize].map(|v| v as u64);
        self.tup_idx = 0;
    }
}

pub trait Hash {
    fn read<'a>(self, key: u16, buf: Arc<PageBuffer>) -> TableIter<HashTable>;
    fn insert(&mut self, key: u16, val: Tuple, buf: Arc<PageBuffer>) -> Result<(), Error>;
}

impl Hash for HashTable {

    fn read<'a>(self, key: u16, buf: Arc<PageBuffer>) -> TableIter<HashTable> {
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

    fn insert(&mut self, key: u16, val: Tuple, buf: Arc<PageBuffer>) -> Result<(), Error> {
        if self.keys[key as usize] == None {
            self.append_block().unwrap();
            self.keys[key as usize] = Some(self.num_blocks - 1);
            return self.insert(key, val, Arc::clone(&buf))
        }
        let block_num = self.keys[key as usize].unwrap();
        let mut page = buf.fetch((self.inode.data_ino as u128)<<64 | (block_num as u128 & 0xFFFFFFFF));
        let mut next;
        {
            let page_read = page.read().unwrap();
            next = page_read.get_next();
            drop(page_read);
        }
        while next.is_some() {
            page = buf.fetch((self.inode.data_ino as u128)<<64 | (next.unwrap() as u128 & SET_64 as u128));
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

    use crate::{buffer::tuple::{DatumTypes, Datum, PageBuffer, Table}, storage::folder::Folder};

    use super::{HashTable, Hash};

    #[test]
    pub fn test_hash_table() {
        let id = "hash_table".to_string();
        let f = Arc::new(Folder::new().unwrap());
        let mut h = HashTable::create(Arc::clone(&f), &id, vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let key = 10;
        let val = vec![Datum::Int(10), Datum::Int(20)];
        let val1 = vec![Datum::Int(10), Datum::Int(30)];
        let buf = Arc::new(PageBuffer::new(10));
        h.insert(key, val.to_vec(), Arc::clone(&buf)).unwrap();
        h.insert(key+1, val1.to_vec(), Arc::clone(&buf)).unwrap();
        let ret: Vec<Vec<Datum>> = h.read(key, Arc::clone(&buf)).collect();
        assert_eq!(ret, vec![val]);
    }
}