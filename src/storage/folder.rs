use std::io::{Write, Read};

use serde::{Serialize, Deserialize};

use super::utils::{create_file, open_file};


#[derive(Serialize, Deserialize)]
pub struct Folder {
    num_tables: u32,
    tables: Vec<(String, u32)>
}

impl Folder {

    pub fn create() {
        let mut file = create_file("folder").unwrap();
        let folder = Folder { num_tables: 0, tables: vec![] };
        file.write_all(&bincode::serialize(&folder).unwrap()).unwrap();
    }

    pub fn new() -> Self {
        let mut folder = open_file("folder").unwrap();
        let mut bytes = Vec::new();
        folder.read_to_end(&mut bytes).unwrap();
        bincode::deserialize(&bytes).unwrap()
    }

    pub fn add(&mut self, table: String) {
        self.tables.push((table, self.num_tables));
        self.num_tables += 1;
    }

    pub fn get(&self, table: &String) -> Option<u32> {
        let t = self.tables.iter().find(|(n, _)| n == table);
        match t {
            Some((_, id)) => Some(id.clone()),
            None => None
        }
    }

    pub fn save(&self) {
        let mut file = create_file("folder").unwrap();
        file.write_all(&bincode::serialize(self).unwrap()).unwrap();
    }
}

#[cfg(test)]
mod tests {
    use super::Folder;

    #[test]
    pub fn test_folder_create() {
        Folder::create();
    }
}