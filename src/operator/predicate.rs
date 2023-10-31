use crate::{buffer::tuple::{DatumTypes, Table, File, Tuple, Hash}, error::Error};


#[derive(Debug, PartialEq)]
pub struct Field {
    pub table: String,
    pub col: String
}

impl Field {
    pub fn new(table: &str, col: &str) -> Self {
        Self { table: table.to_string(), col:col.to_string() }
    }

    pub fn get_type(&self) -> Result<DatumTypes, Error> {
        let t = Table::new(&self.table)?;
        t.get_schema().iter().find(|(col, _)| col == &(self.table.clone() + "." + &self.col)).map(|(_, typ)| typ.clone()).ok_or(Error::ColumnDoesNotExist)
    }

    pub fn generate_hash(&self) -> Result<impl Fn(&Tuple) -> u16, Error> {
        let t = Table::new(&self.table)?;
        let idx = t.get_schema().iter().enumerate().find(|(_, (col, _))| col == &(self.table.clone() + "." + &self.col)).map(|(idx, _)| idx).ok_or(Error::ColumnDoesNotExist)?;
        Ok(move |tuple: &Tuple| {
            tuple[idx].hash()
        })
    }
}

#[derive(Debug, PartialEq)]
pub struct Equal {
    pub l: Field,
    pub r: Field
}

impl Equal {
    pub fn new(l: Field, r: Field) -> Self {
        Self { l, r }
    }

    pub fn generate_hashes(&self) -> Result<(impl Fn(&Tuple) -> u16, impl Fn(&Tuple) -> u16), Error> {
        Ok((self.l.generate_hash()?, self.r.generate_hash()?))
    }
}

#[derive(Debug, PartialEq)]
pub enum Predicate {
    Equal(Equal)
}

impl Predicate {
    pub fn generate_hashes(&self) -> Result<(impl Fn(&Tuple) -> u16, impl Fn(&Tuple) -> u16), Error> {
        match self {
            Self::Equal(e) => e.generate_hashes()
        }
    }
}