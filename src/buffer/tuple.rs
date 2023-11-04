use std::{io::Write, sync::{RwLock, Arc}, fmt::Debug, ptr};

use serde::{Serialize, Deserialize};

use crate::{storage::{utils::{create_file, append_block, delete_file}, folder::{Folder, TableInode}, disk_manager::SET_64}, error::{Error, PageError}};

use super::{Buff, page::{TupleCRUD, Page}, Buffer, BufferInner, Clock};

pub type Tuple = Vec<Datum>;
pub type Schema = Vec<(String, DatumTypes)>;
pub type PageBuffer = Buffer<RwLock<Page>, BufferInner<RwLock<Page>>, Clock>;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub enum Datum {
    Int(i32),
    Float(f32)
}

impl Hash for Datum {
    fn hash(&self) -> u16 {
        match self {
            Self::Int(i) => i.hash(),
            Self::Float(f) => f.hash()
        }
    }
}

impl Hash for i32 {
    fn hash(&self) -> u16 {
        *self as u16
    }
}

impl Hash for f32 {
    fn hash(&self) -> u16 {
        *self as u16
    }
}

pub trait Hash {
    fn hash(&self) -> u16;
}


#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub enum DatumTypes {
    Int,
    Float
}

impl DatumTypes {
    fn serialized_size(&self) -> u64 {
        match *self {
            DatumTypes::Int => bincode::serialized_size(&(0 as i32)).unwrap(),
            DatumTypes::Float => bincode::serialized_size(&(0 as f32)).unwrap()
        }
    }

    pub fn parse(typ: &str) -> Result<Self, Error> {
        match typ.to_ascii_uppercase().as_str() {
            "INT" => Ok(Self::Int),
            "FLOAT" => Ok(Self::Float),
            _ => Err(Error::ParseError)
        }
    }
}

pub trait DatumSerde {
    fn encode(&self, datum: &Datum) -> Option<Vec<u8>>;
    fn decode(&self, bytes: &[u8]) -> Option<Datum>; 
}

impl DatumSerde for DatumTypes {

    fn encode(&self, datum: &Datum) -> Option<Vec<u8>> {
        match (self, datum) {
            (DatumTypes::Int, Datum::Int(v)) => Some(bincode::serialize(v).unwrap()),
            (DatumTypes::Float, Datum::Float(v)) => Some(bincode::serialize(v).unwrap()),
            _ => None
        }
    }

    fn decode(&self, bytes: &[u8]) -> Option<Datum> {
        match *self {
            DatumTypes::Int => Some(Datum::Int(bincode::deserialize(bytes).unwrap())),
            DatumTypes::Float => Some(Datum::Float(bincode::deserialize(bytes).unwrap()))
        }
    }
}

pub trait Table {
    fn inode(&self) -> TableInode;
    fn set_inode(&mut self, inode: TableInode);
    fn temp(&self) -> bool;
    fn set_temp(&mut self, temp: bool);
    fn schema(&self) -> Schema;
    fn set_schema(&mut self, schema: Schema);
    fn create(f: Arc<Folder>, name: &str, schema: Schema) -> Result<Self, Error> where Self: Sized;
    fn create_temp(f: Arc<Folder>, schema: Schema) -> Result<Self, Error> where Self: Sized;
    fn new(f: Arc<Folder>, name: &str) -> Result<Self, Error> where Self: Sized;
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct RowTable {
    pub inode: TableInode,
    pub temp: bool,
    pub num_blocks: u64,
    pub schema: Schema
}

impl Drop for RowTable {
    fn drop(&mut self) {
        if self.temp() { 
            delete_file(&self.inode.data_ino.to_string()).unwrap();
        }
    }
}

impl Table for RowTable {
    fn inode(&self) -> TableInode {
        self.inode.clone()
    }

    fn set_inode(&mut self, inode: TableInode) {
        self.inode = inode;
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

impl Default for RowTable {
    fn default() -> Self {
        RowTable {
            inode: TableInode::new(0, 0),
            temp: false,
            num_blocks: 0,
            schema: vec![]
        }
    }
}

impl RowTable {
    fn append_block(&mut self) -> Option<()> {
        append_block(&self.inode.data_ino.to_string()).unwrap();
        self.num_blocks += 1;
        let mut h_file = create_file(&(self.inode.head_ino.to_string())).expect("could not create header file");
        h_file.write_all(&bincode::serialize(&self).unwrap()).unwrap();
        Some(())
    }
}

pub trait TupleOps {
    fn add(&mut self, page_buffer: Arc<PageBuffer>, tuple: Tuple) -> Result<(), Error>;
}

impl TupleOps for RowTable {
    fn add(&mut self, p_buf: Arc<PageBuffer>, tuple: Tuple) -> Result<(), Error> {
        if self.num_blocks == 0 {self.append_block();}
        let page = p_buf.fetch(((self.inode.data_ino as u128)<<64) | ((self.num_blocks - 1) & SET_64) as u128);
        let mut p = page.write().unwrap();
        
        let bind = p.add(tuple.to_vec(), &self.schema);
        drop(p);
        let ret = match bind {
            Ok(()) => Ok(()),
            Err(Error::PageError(PageError::OutOfBounds)) => {
                self.append_block().unwrap();
                self.add(p_buf, tuple)
            },
            Err(e) => Err(e)
        };
        ret
    }
}

pub struct TableIter<T: Table> {
    pub block_num: Option<u64>,
    pub buf: Arc<PageBuffer>,
    pub tup_idx: u16,
    pub table: T,
    pub page: *const RwLock<Page>,
    pub on_page_end: fn(&mut TableIter<T>) -> bool
}

impl RowTable {
    pub fn iter(&self, buf: Arc<PageBuffer>) -> TableIter<Self> {
        TableIter { 
            block_num: Some(0), 
            buf,
            tup_idx: 0, 
            table: self.clone(), 
            page: ptr::null(),
            on_page_end: |i| {
                *i.block_num.as_mut().unwrap() += 1;
                i.tup_idx = 0;
                if i.block_num.unwrap() >= i.table.num_blocks {true} else {false}
            } 
        }
    }
}

pub trait Operator: Iterator<Item = Tuple> {
    fn get_schema(&self) -> Schema;
}

impl<T: Table> Iterator for TableIter<T> {

    type Item = Tuple;

    fn next(&mut self) -> Option<Self::Item>{
        let Some(block_num) = self.block_num else { return None; };
        if self.page.is_null() {
            self.page = self.buf.fetch(((self.table.inode().data_ino as u128) << 64) | (block_num & SET_64) as u128) as *const RwLock<Page>;
        } else {
            let page_id;
            let page = unsafe {self.page.as_ref().unwrap().read().unwrap()};
            page_id = page.page_id;
            drop(page);
            if page_id != Some(((self.table.inode().data_ino as u128) << 64) | (block_num & SET_64) as u128) {
                self.page = self.buf.fetch(((self.table.inode().data_ino as u128) << 64) | (block_num & SET_64) as u128) as *const RwLock<Page>;
            }
        }
        let tup = unsafe {PageIter::iter(self.page.as_ref().unwrap(), &self.table.schema()).nth(self.tup_idx as usize)};
        match tup {
            Ok(Some(t)) => {
                self.tup_idx += 1;
                Some(t)
            },
            Ok(None) => {
                self.tup_idx += 1;
                self.next()
            },
            Err(_) => {
                if (self.on_page_end)(self) { return None; }
                self.page = self.buf.fetch(((self.table.inode().data_ino as u128) << 64) | (block_num & SET_64) as u128) as *const RwLock<Page>;
                self.next()
            }
        }
    }
}

pub struct PageIter<'a> {
    tup_idx: u16,
    tup_siz: u16,
    schema: Schema,
    page: &'a RwLock<Page>
}

impl PageIter<'_> {
    pub fn iter<'a>(page: &'a RwLock<Page>, schema: &Schema) -> PageIter<'a> {
        let tup_siz = schema
            .iter()
            .map(|(_, x)| x.serialized_size())
            .reduce(|acc, siz| acc + siz).unwrap() as u16;

        PageIter { tup_idx: 0, tup_siz, schema: schema.to_vec(), page }
    }

    pub fn nth(&mut self, n: usize) -> Result<Option<Tuple>, Error> {
        self.tup_idx += n as u16;
        let bytes = self.page.read().unwrap().read(self.tup_idx, self.tup_siz)?;

        let Some(bytes) = bytes else { return Ok(None); };
        
        Ok(Some(self.schema
                .iter()
                .scan(0, |pre, (_, x)| {
                    let siz = x.serialized_size();
                    *pre += siz;
                    Some(x.decode(&bytes[(*pre-siz) as usize ..*pre as usize]).expect("could not decode tuple element"))
                })
                .collect()))
    }
}

impl Iterator for PageIter<'_> {
    type Item = Tuple;
    
    fn next(&mut self) -> Option<Self::Item> {
        let Ok(bytes)= self.page.read().unwrap().read(self.tup_idx, self.tup_siz) else { return None; };
        self.tup_idx += 1;

        let Some(bytes) = bytes else { return self.next(); };

        Some(self.schema
                .iter()
                .scan(0, |pre, (_, x)| {
                    let siz = x.serialized_size();
                    *pre += siz;
                    Some(x.decode(&bytes[(*pre-siz) as usize ..*pre as usize]).expect("could not decode tuple element"))
                })
                .collect())
    }
}

#[cfg(test)]
mod tests {
    use std::{vec, sync::Arc};

    use crate::{buffer::{tuple::{PageIter, Table, PageBuffer}, Buff}, storage::folder::Folder};

    use super::{RowTable, DatumTypes, TupleOps, Datum};

    
    #[test]
    fn test_table_create() {
        let t_name = "test_table_create".to_string();
        let f = Arc::new(Folder::new().unwrap());
        let t = RowTable::create(Arc::clone(&f), &t_name, vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        assert_eq!(t, RowTable { inode: t.inode(), temp: false, num_blocks: 0, schema: vec![(t_name.clone()+"."+"a", DatumTypes::Int), (t_name.clone()+"."+"b", DatumTypes::Int)]});
    }

    #[test]
    fn test_page_itr_nth() {
        let id = "page_itr_nth".to_string();
        let f = Arc::new(Folder::new().unwrap());
        let mut t = RowTable::create(Arc::clone(&f), &id, vec![("a".to_string(), DatumTypes::Int), ("b".to_string(), DatumTypes::Int)]).unwrap();
        let buf = Arc::new(PageBuffer::new(10));
        let mut tuple = vec![Datum::Int(10), Datum::Int(20)];
        t.add(Arc::clone(&buf), tuple).unwrap();
        tuple = vec![Datum::Int(10), Datum::Int(30)];
        t.add(Arc::clone(&buf), tuple).unwrap();
        let bind = buf.fetch((t.inode().data_ino as u128) << 64);
        let mut itr = PageIter::iter(bind, &t.schema);

        assert_eq!(itr.nth(1).unwrap(), Some(vec![Datum::Int(10), Datum::Int(30)]));
        assert!(itr.nth(1).is_err());
    }
}