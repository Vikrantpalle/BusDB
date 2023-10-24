use std::{io::{Write, Read}, sync::{RwLock, Arc}};

use serde::{Serialize, Deserialize};

use crate::storage::{utils::{create_file, open_file, append_block}, folder::Folder};

use super::{ClockBuffer, Buffer, page::{TupleCRUD, Page}};

pub const HFILE_SUF: &str = "_h";

pub type Tuple = Vec<Datum>;
pub type Schema = Vec<(String, DatumTypes)>;

#[derive(Debug, PartialEq, Clone)]
pub enum Datum {
    Int(i32),
    Float(f32)
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

    pub fn parse(typ: &str) -> Self {
        match typ.to_ascii_uppercase().as_str() {
            "INT" => Self::Int,
            "FLOAT" => Self::Float,
            _ => panic!()
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

pub trait File {
    fn get_id(&self) -> u32;
    fn get_schema(&self) -> Schema;
}

#[derive(Serialize, Deserialize, PartialEq, Debug, Clone)]
pub struct Table {
    pub id: u32,
    pub num_blocks: u64,
    pub schema: Schema
}

impl File for Table {
    fn get_id(&self) -> u32 {
        self.id
    }

    fn get_schema(&self) -> Schema {
        self.schema.to_vec()
    }
}

impl Table {
    pub fn create(name: String, schema: Schema) {
        let mut f = Folder::new();
        f.add(name.clone());
        let id = f.get(&name).unwrap();
        f.save();
        create_file(&id.to_string()).unwrap();
        append_block(&id.to_string()).unwrap();
        let mut h_file = create_file(&(id.to_string() + HFILE_SUF)).expect("could not create header file");
        let table = Table {
            id,
            num_blocks: 1,
            schema
        };
        h_file.write_all(&bincode::serialize(&table).unwrap()).unwrap();
    }

    pub fn new(name: String) -> Self {
        let f = Folder::new();
        let id = f.get(&name).unwrap();
        let mut h_file = open_file(&(id.to_string() + HFILE_SUF)).expect("could not open header file");
        let mut bytes = Vec::new();
        h_file.read_to_end(&mut bytes).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }
}

impl Table {
    fn append_block(&mut self) -> Option<()> {
        append_block(&self.id.to_string()).unwrap();
        self.num_blocks += 1;
        let mut h_file = create_file(&(self.id.to_string() + HFILE_SUF)).expect("could not create header file");
        h_file.write_all(&bincode::serialize(&self).unwrap()).unwrap();
        Some(())
    }
}

pub trait TupleOps {
    fn add(&mut self, page_buffer: &mut ClockBuffer, tuple: Tuple) -> Option<()>;
    fn read(&self, page_buffer: &mut ClockBuffer, index: u16) -> Tuple;
}

impl TupleOps for Table {
    fn add(&mut self, p_buf: &mut ClockBuffer, tuple: Tuple) -> Option<()> {
        let page = p_buf.fetch(((self.id as u64)<<32) | ((self.num_blocks - 1) & 0xFFFFFFFF));
        let mut p = page.write().unwrap();
        
        let bind = p.add(tuple.to_vec(), &self.schema);
        drop(p);
        let ret = match bind {
            Some(_) => Some(()),
            None => {
                self.append_block().unwrap();
                self.add(p_buf, tuple)
            }
        };
        ret
    }

    fn read(&self, p_buf: &mut ClockBuffer, index: u16) -> Vec<Datum> {
        let page = p_buf.fetch((self.id as u64)<<32);
        let p = page.read().unwrap(); // ! add block number 
        let bytes= p.read(index, self.schema
                                        .iter()
                                        .map(|(_, x)| x.serialized_size())
                                        .reduce(|acc, siz| acc + siz).unwrap() as u16).unwrap().unwrap();
        self.schema
                .iter()
                .scan(0, |pre, (_, x)| {
                    let siz = x.serialized_size();
                    *pre += siz;
                    Some(x.decode(&bytes[(*pre-siz) as usize ..*pre as usize]).expect("could not decode tuple element"))
                })
                .collect()
    }
}

pub struct TableIter<T: File> {
    pub block_num: Option<u64>,
    pub tup_idx: u16,
    pub table: T,
    pub page: Option<Arc<RwLock<Page>>>,
    pub on_page_end: fn(&mut TableIter<T>) -> bool
}

impl Table {
    pub fn iter(&self) -> TableIter<Self> {
        TableIter { 
            block_num: Some(0), 
            tup_idx: 0, 
            table: self.clone(), 
            page: None,
            on_page_end: |i| {
                *i.block_num.as_mut().unwrap() += 1;
                i.tup_idx = 0;
                if i.block_num.unwrap() >= i.table.num_blocks {true} else {false}
            } 
        }
    }
}

pub trait Operator {
    type Item;
    fn next(&mut self, p_buf: &mut ClockBuffer) -> Option<Self::Item>;
    fn get_schema(&self) -> Schema;

    fn collect(&mut self, p_buf: &mut ClockBuffer) -> Vec<Self::Item> {
        let mut container = Vec::new();
        let mut t = self.next(p_buf);
        while t.is_some() {
            container.push(t.unwrap());
            t = self.next(p_buf);
        }
        container
    }

    fn for_each<F>(&mut self, mut func: F, p_buf: &mut ClockBuffer)
    where F: FnMut(Self::Item) {
        let mut t = self.next(p_buf);
        while t.is_some() {
            func(t.unwrap());
            t = self.next(p_buf);
        }
    } 
}

impl<T: File> Operator for TableIter<T> {

    type Item = Tuple;

    fn next(&mut self, p_buf: &mut ClockBuffer) -> Option<Self::Item>{
        let Some(block_num) = self.block_num else { return None; };
        if self.page.is_none() || self.page.as_ref().unwrap().read().unwrap().page_id != Some(((self.table.get_id() as u64) << 32) | (block_num & 0xFFFFFFFF)) {
            self.page = Some(p_buf.fetch(((self.table.get_id() as u64) << 32) | (block_num & 0xFFFFFFFF)))
        }
        let tup = PageIter::iter(Arc::clone(&self.page.as_ref().unwrap()), &self.table.get_schema()).nth(self.tup_idx as usize);
        match tup {
            Ok(Some(t)) => {
                self.tup_idx += 1;
                Some(t)
            },
            Ok(None) => {
                self.tup_idx += 1;
                self.next(p_buf)
            },
            Err(_) => {
                if (self.on_page_end)(self) { return None; }
                self.page = Some(p_buf.fetch(((self.table.get_id() as u64) << 32) | (block_num & 0xFFFFFFFF)));
                self.next(p_buf)
            }
        }
    }

    fn get_schema(&self) -> Schema {
        self.table.get_schema()
    }
}

pub struct PageIter {
    tup_idx: u16,
    tup_siz: u16,
    schema: Schema,
    page: Arc<RwLock<Page>>
}

impl PageIter {
    pub fn iter(page: Arc<RwLock<Page>>, schema: &Schema) -> PageIter {
        let tup_siz = schema
            .iter()
            .map(|(_, x)| x.serialized_size())
            .reduce(|acc, siz| acc + siz).unwrap() as u16;

        PageIter { tup_idx: 0, tup_siz, schema: schema.to_vec(), page }
    }

    pub fn nth(&mut self, n: usize) -> Result<Option<Tuple>, ()> {
        self.tup_idx += n as u16;
        let Ok(bytes)= self.page.read().unwrap().read(self.tup_idx, self.tup_siz) else { return Err(()); };

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

impl Iterator for PageIter {
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
    use std::vec;

    use crate::buffer::{ClockBuffer, Buffer, tuple::{PageIter, File}};

    use super::{Table, DatumTypes, TupleOps, Datum};

    
    #[test]
    fn test_table_create() {
        let t_name = "test_table_create".to_string();
        Table::create(t_name.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]);
        let t = Table::new(t_name);
        assert_eq!(t, Table{ id: t.get_id(), num_blocks: 1, schema: vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]});
    }

    #[test]
    fn test_page_itr_nth() {
        let t_id = "test_page_itr_nth".to_string();
        Table::create(t_id.clone(), vec![("a".to_string(), DatumTypes::Int), ("b".to_string(), DatumTypes::Int)]);
        let mut t = Table::new(t_id);
        let mut buf = ClockBuffer::new(101);
        let mut tuple = vec![Datum::Int(10), Datum::Int(20)];
        t.add(&mut buf, tuple);
        tuple = vec![Datum::Int(10), Datum::Int(30)];
        t.add(&mut buf, tuple);
        let bind = buf.fetch((t.get_id() as u64) << 32);
        let mut itr = PageIter::iter(bind, &t.schema);

        assert_eq!(itr.nth(1), Ok(Some(vec![Datum::Int(10), Datum::Int(30)])));
        assert_eq!(itr.nth(1), Err(()));
    }
}