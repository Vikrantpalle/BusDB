use crate::{storage::{Block, LOCSIZ, Flags}, error::{Error, PageError}};

use super::tuple::{Tuple, Schema, DatumSerde};

#[derive(Clone, Debug)]
pub struct Page {
    pub page_id: Option<u64>,
    pub block: Option<Block>
}

impl Page {
    pub fn new() -> Self { 
        Self { page_id: None, block: None }
    }
}

pub trait TupleCRUD {
    fn write(&mut self, data: &[u8]) -> Result<(), Error>;
    fn read(&self, tup_idx: u16, tup_siz: u16) -> Result<Option<Vec<u8>>, Error>;
    fn update(&mut self, tup_idx: u16, data: &[u8]) -> Result<(), Error>;
    fn delete(&mut self, tup_idx: u16) -> Result<(), Error>;
} 

impl TupleCRUD for Page {
    fn write(&mut self, data: &[u8]) -> Result<(), Error> {
        let Some(block) = &mut self.block else {return Err(Error::PageError(PageError::NoBlock))};
        let write_len = data.len() as u16;

        if block.lower + LOCSIZ >= block.upper - write_len {return Err(Error::PageError(PageError::OutOfBounds));}

        block.upper -= write_len;
        block.lower += LOCSIZ;
        block.data[(block.lower-LOCSIZ) as usize..block.lower as usize].copy_from_slice(&block.upper.to_le_bytes());
        block.data[block.upper as usize..(block.upper+write_len) as usize].copy_from_slice(&data);
        block.set_flag(&Flags::Dirty);
        Ok(())
    }

    fn read(&self, tup_idx: u16, tup_siz: u16) -> Result<Option<Vec<u8>>, Error> {
        let Some(block) = &self.block else {return Err(Error::PageError(PageError::NoBlock))};
        let start = tup_idx * LOCSIZ;
        if start >= block.lower {return Err(Error::PageError(PageError::OutOfBounds))}

        let tup_loc = block.data[start as usize] as u16 | (block.data[start as usize+1] as u16)<<8;
        if tup_loc == 0xFFFF { return Ok(None);}
        Ok(Some(block.data[tup_loc as usize..(tup_loc+tup_siz) as usize].to_vec()))
    }

    fn update(&mut self, tup_idx: u16, data: &[u8]) -> Result<(), Error> {
        let Some(block) = &mut self.block else {return Err(Error::PageError(PageError::NoBlock))};
        let start = tup_idx * LOCSIZ;
        if start >= block.lower {return Err(Error::PageError(PageError::OutOfBounds));}

        let tup_loc = block.data[start as usize] as u16 | (block.data[start as usize+1] as u16)<<8;
        block.data[tup_loc as usize..(tup_loc+4) as usize].copy_from_slice(&data);
        Ok(())
    }

    fn delete(&mut self, tup_idx: u16) -> Result<(), Error> {
        let Some(block) = &mut self.block else {return Err(Error::PageError(PageError::NoBlock))};
        let start = tup_idx * LOCSIZ;
        if start >= block.lower {return Err(Error::PageError(PageError::OutOfBounds));}
        
        block.data[start as usize] = 0xFF;
        block.data[start as usize + 1] =0xFF;
        Ok(())
    }
}

impl Page {

    pub fn add(&mut self, tuple: Tuple, schema: &Schema) -> Result<(), Error> {
        if schema.len() != tuple.len() {return Err(Error::PageError(PageError::InvalidTuple));}
        let bytes: Vec<u8> = schema
                                    .iter()
                                    .zip(tuple.iter())
                                    .flat_map(|((_, ty), val)| ty.encode(val).expect("type mismatch"))
                                    .collect();
        self.write(&bytes)
    }

    pub fn toggle_dirty(&mut self) -> Option<()> {
        let Some(b) = &mut self.block else { return None; };
        b.toggle_flag(&Flags::Dirty);
        Some(())
    }

    pub fn is_dirty(&self) -> bool {
        let Some(b) = &self.block else { return false; };
        b.check_flag(&Flags::Dirty)
    }

    pub fn has_next(&self) -> bool {
        let Some(b) = &self.block else { return false; };
        b.check_flag(&Flags::Next);
        return false;
    }

    pub fn get_next(&self) -> Option<u32> {
        if !self.has_next() { return None; }
        let Some(b) = &self.block else { return None };
        Some(b.get_next())
    }

    pub fn set_next(&mut self, val: u32) -> Option<()> {
        let Some(b) = &mut self.block else { return None };
        b.set_next(val);
        Some(())
    }
}

impl Default for Page {
    fn default() -> Self {
        Self {
            page_id: None,
            block: None
        }
    }
}


#[cfg(test)]
mod tests {

    use crate::{buffer::TupleCRUD, storage::{DATSIZ, Block}};

    use super::Page;

    #[test]
    fn test_tuple_write() {
        let mut p = Page {
            page_id: Some(0),
            block: Some(Block::new(0))
        };
        let tuple: (u16, u16) = (12, 14);

        p.write(&bincode::serialize(&tuple).unwrap()).unwrap();

        let Some(b) = &p.block else {panic!()};
        assert_eq!(b.data[..2], [231, 31]);
        assert_eq!(tuple, bincode::deserialize(&b.data[b.data.len()-4..]).unwrap());
        assert_eq!(b.lower, 2);
        assert_eq!(b.upper, DATSIZ as u16-4);
    }

    #[test]
    fn test_tuple_read() {
        let block = Block::new(0);
        let mut p = Page {
            page_id: Some(0),
            block: Some(block.clone())
        };
        let tuple: (u16, u16) = (12, 14);
        p.write(&bincode::serialize(&tuple).unwrap()).unwrap();

        let t = p.read(0, bincode::serialized_size(&tuple).unwrap() as u16).unwrap().unwrap();

        assert_eq!(tuple, bincode::deserialize(&t).unwrap());
    }

    #[test]
    fn test_tuple_update() {
        let mut p = Page {
            page_id: Some(0),
            block: Some(Block::new(0))
        };
        let mut tuple: (u16, u16) = (12, 14);
        p.write(&bincode::serialize(&tuple).unwrap()).unwrap();

        tuple = (11, 15);
        p.update(0, &bincode::serialize(&tuple).unwrap()).unwrap();
        let Some(b) = &p.block else {panic!()};

        assert_eq!(tuple, bincode::deserialize(&b.data[b.data.len()-4..]).unwrap());
    }

    #[test]
    fn test_tuple_delete() {
        let mut p = Page {
            page_id: Some(0),
            block: Some(Block::new(0))
        };
        let tuple: (u16, u16) = (12, 14);
        p.write(&bincode::serialize(&tuple).unwrap()).unwrap();

        p.delete(0).unwrap();

        let Some(b) = &p.block else {panic!()};

        assert_eq!(b.data[..2], [0xFF, 0xFF]);
    }

}