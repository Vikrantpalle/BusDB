use std::{io::{Write, Read}, sync::{RwLock, Arc}, fmt::Debug};

use serde::{Serialize, Deserialize};

use crate::{storage::{utils::{create_file, open_file, append_block}, folder::Folder}, error::{Error, PageError}};

use super::{ClockBuffer, Buffer, page::{TupleCRUD, Page}};

pub const HFILE_SUF: &str = "_h";

pub type Tuple = Vec<Datum>;
pub type Schema = Vec<(String, DatumTypes)>;

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
    pub fn create(name: String, schema: Schema) -> Result<(), Error> {
        let mut f = Folder::new()?;
        f.add(name.clone());
        let id = f.get(&name).unwrap();
        f.save()?;
        create_file(&id.to_string())?;
        append_block(&id.to_string())?;
        let mut h_file = create_file(&(id.to_string() + HFILE_SUF)).expect("could not create header file");
        let schema = schema.into_iter().map(|t| (name.clone()+"."+&t.0, t.1)).collect();
        let table = Table {
            id,
            num_blocks: 1,
            schema
        };
        h_file.write_all(&bincode::serialize(&table).unwrap())?;
        Ok(())
    }

    pub fn new(name: &str) -> Result<Self, Error> {
        let f = Folder::new()?;
        let id = f.get(name).ok_or(Error::InvalidName)?;
        let mut h_file = open_file(&(id.to_string() + HFILE_SUF))?;
        let mut bytes = Vec::new();
        h_file.read_to_end(&mut bytes).unwrap();
        Ok(bincode::deserialize(&bytes).unwrap())
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
    fn add(&mut self, page_buffer: &mut ClockBuffer, tuple: Tuple) -> Result<(), Error>;
}

impl TupleOps for Table {
    fn add(&mut self, p_buf: &mut ClockBuffer, tuple: Tuple) -> Result<(), Error> {
        let page = p_buf.fetch(((self.id as u64)<<32) | ((self.num_blocks - 1) & 0xFFFFFFFF));
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

pub trait Operate {
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

impl<T: File> Operate for TableIter<T> {

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
        Table::create(t_name.clone(), vec![("a".into(), DatumTypes::Int), ("b".into(), DatumTypes::Int)]).unwrap();
        let t = Table::new(&t_name).unwrap();
        assert_eq!(t, Table{ id: t.get_id(), num_blocks: 1, schema: vec![(t_name.clone()+"."+"a", DatumTypes::Int), (t_name.clone()+"."+"b", DatumTypes::Int)]});
    }

    #[test]
    fn test_page_itr_nth() {
        let t_id = "test_page_itr_nth".to_string();
        Table::create(t_id.clone(), vec![("a".to_string(), DatumTypes::Int), ("b".to_string(), DatumTypes::Int)]).unwrap();
        let mut t = Table::new(&t_id).unwrap();
        let mut buf = ClockBuffer::new(101);
        let mut tuple = vec![Datum::Int(10), Datum::Int(20)];
        t.add(&mut buf, tuple).unwrap();
        tuple = vec![Datum::Int(10), Datum::Int(30)];
        t.add(&mut buf, tuple).unwrap();
        let bind = buf.fetch((t.get_id() as u64) << 32);
        let mut itr = PageIter::iter(bind, &t.schema);

        assert_eq!(itr.nth(1).unwrap(), Some(vec![Datum::Int(10), Datum::Int(30)]));
        assert!(itr.nth(1).is_err());
    }
}