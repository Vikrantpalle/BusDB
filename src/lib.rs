#![allow(non_snake_case)]

pub mod storage;
pub mod buffer;
pub mod optree;
pub mod index;
pub mod parser;

pub fn add(left: usize, right: usize) -> usize {
    left + right
}

#[cfg(test)]
mod tests {

    
}
