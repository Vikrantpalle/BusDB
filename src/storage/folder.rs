use std::{io::{Write, Read}, os::windows::prelude::MetadataExt, sync::RwLock};

use serde::{Serialize, Deserialize, de::DeserializeOwned};

use crate::{error::Error, buffer::{tuple::{Table, Schema}, Buffer, BufferInner, Clock}};

use super::utils::{create_file, open_file, rename_file, write_file, delete_file};

pub type HeadBuffer = Buffer<RwLock<Option<Box<dyn Table + Send + Sync>>>, BufferInner<RwLock<Option<Box<dyn Table + Send + Sync>>>>, Clock>;

impl Default for HeadBuffer {
    fn default() -> Self {
        HeadBuffer::new(10)
    }
}

#[derive(Serialize, Deserialize, Debug, PartialEq, Clone)]
pub struct TableInode {
    pub head_ino: u64,
    pub data_ino: u64
}

impl TableInode {
    pub fn new(head_ino: u64, data_ino: u64) -> Self { Self { head_ino, data_ino } }
}

#[derive(Serialize, Deserialize)]
pub struct Folder {
    num_tables: u64,
    tables: RwLock<Vec<(String, TableInode)>>,
    #[serde(skip)]
    buf: HeadBuffer
}

impl Folder {

    pub fn create() -> Result<(), Error> {
        let mut file = create_file("folder")?;
        let folder = Folder { num_tables: 0, tables: RwLock::new(vec![]), buf: HeadBuffer::default() };
        file.write_all(&bincode::serialize(&folder).unwrap())?;
        Ok(())
    }
 
    pub fn new() -> Result<Self, Error> {
        let mut folder = open_file("folder")?;
        let mut bytes = Vec::new();
        folder.read_to_end(&mut bytes).unwrap();
        Ok(bincode::deserialize(&bytes).unwrap())
    }

    pub fn create_file() -> Result<u64, std::io::Error> {
        let f = create_file("temp")?;
        let meta = f.metadata()?;
        let inode = meta.file_index().unwrap();
        rename_file("temp", &inode.to_string())?;
        Ok(inode)
    }

    pub fn create_table<T: Table + Default + Serialize>(&self, name: &str, schema: Schema) -> Result<T, std::io::Error> {
        let data_ino = Self::create_file()?;
        let head_ino = Self::create_file()?;
        let mut table = T::default();
        let schema = schema.into_iter().map(|t| (name.to_owned()+"."+&t.0, t.1)).collect();
        table.set_inode(TableInode::new(head_ino, data_ino));
        table.set_schema(schema);
        let mut f = write_file(&head_ino.to_string())?;
        f.write_all(&bincode::serialize(&table).unwrap())?;
        let mut tables = self.tables.write().unwrap();
        tables.push((name.into(), TableInode::new(head_ino, data_ino)));
        Ok(table)
    }

    pub fn fetch_table<T: DeserializeOwned>(&self, name: &str) -> Result<Option<T>, Error> {
        let tables = self.tables.read().unwrap();
        let head_ino = tables.iter().find(|(n, _)| n == name).map(|(_, inode)| inode.head_ino.clone()).ok_or(Error::TableDoesNotExist)?;
        drop(tables);
        let mut f = open_file(&head_ino.to_string())?;
        let mut bytes = Vec::new();
        f.read_to_end(&mut bytes)?;
        Ok(Some(bincode::deserialize(&bytes).unwrap()))
    }

    pub fn delete_temp_table(inode: TableInode) -> Result<(), Error> {
        delete_file(&inode.head_ino.to_string())?;
        delete_file(&inode.data_ino.to_string())?;
        Ok(())
    }

    pub fn save(&self) -> Result<(), Error> {
        let mut file = create_file("folder")?;
        file.write_all(&bincode::serialize(self).unwrap())?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::Folder;

    #[test]
    pub fn test_folder_create() {
        Folder::create().unwrap();
    }
}