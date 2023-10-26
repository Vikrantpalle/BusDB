use std::io::{Write, Read};

use serde::{Serialize, Deserialize};

use crate::error::Error;

use super::utils::{create_file, open_file};


#[derive(Serialize, Deserialize)]
pub struct Folder {
    num_tables: u32,
    tables: Vec<(String, u32)>
}

impl Folder {

    pub fn create() -> Result<(), Error> {
        let mut file = create_file("folder")?;
        let folder = Folder { num_tables: 0, tables: vec![] };
        file.write_all(&bincode::serialize(&folder).unwrap())?;
        Ok(())
    }

    pub fn new() -> Result<Self, Error> {
        let mut folder = open_file("folder")?;
        let mut bytes = Vec::new();
        folder.read_to_end(&mut bytes).unwrap();
        Ok(bincode::deserialize(&bytes).unwrap())
    }

    pub fn add(&mut self, table: String) {
        self.tables.push((table, self.num_tables));
        self.num_tables += 1;
    }

    pub fn get(&self, table: &String) -> Option<u32> {
        self.tables.iter().find(|(n, _)| n == table).map(|(_, id)| id.clone())
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