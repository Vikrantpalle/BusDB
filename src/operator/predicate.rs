use std::sync::Arc;

use crate::{buffer::tuple::{DatumTypes, RowTable, Table, Tuple, Hash, Schema}, error::Error, storage::folder::Folder};


#[derive(Debug, PartialEq, Clone)]
pub struct Field {
    pub table: String,
    pub col: String
}

impl Field {
    pub fn new(table: &str, col: &str) -> Self {
        Self { table: table.to_string(), col:col.to_string() }
    }

    pub fn get_type(&self, f: Arc<Folder>) -> Result<DatumTypes, Error> {
        let t = RowTable::new(Arc::clone(&f), &self.table)?;
        t.schema().iter().find(|(col, _)| col == &(self.table.clone() + "." + &self.col)).map(|(_, typ)| typ.clone()).ok_or(Error::ColumnDoesNotExist)
    }
}

#[derive(Debug, PartialEq, Clone)]
pub struct Equal {
    pub l: Field,
    pub r: Field
}

impl Equal {
    pub fn new(l: Field, r: Field) -> Self {
        Self { l, r }
    }

    pub fn generate_hashes(&self, f: Arc<Folder>, schema: &Schema) -> Result<(impl Fn(&Tuple) -> u16, impl Fn(&Tuple) -> u16), Error> {
        let l_len = RowTable::new(Arc::clone(&f), &self.l.table)?.schema().len();
        let l_idx = schema.iter().enumerate().find(|(_, (col, _))| col == &(self.l.table.clone() + "." + &self.l.col)).map(|(idx, _)| idx).ok_or(Error::ColumnDoesNotExist)?;
        let r_idx = schema.iter().enumerate().find(|(_, (col, _))| col == &(self.r.table.clone() + "." + &self.r.col)).map(|(idx, _)| idx).ok_or(Error::ColumnDoesNotExist)? - l_len;
        Ok((
            move |tuple: &Tuple| {
                tuple[l_idx].hash()
            },
            move |tuple: &Tuple| {
                tuple[r_idx].hash()
            }
        ))
    }
}

#[derive(Debug, PartialEq, Clone)]
pub enum Predicate {
    Equal(Equal)
}

impl Predicate {
    pub fn generate_hashes(&self, f: Arc<Folder>, schema: &Schema) -> Result<(impl Fn(&Tuple) -> u16, impl Fn(&Tuple) -> u16), Error> {
        match self {
            Self::Equal(e) => e.generate_hashes(Arc::clone(&f), schema)
        }
    }
}