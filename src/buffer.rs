#![allow(dead_code)]

use std::{fmt::Display, cell::RefCell, rc::Rc};

use crate::storage::disk_manager::{self, write_block};

pub mod page;
pub mod tuple;
use page::*;

pub trait Store {
    type Item;
    fn new(size: usize) -> Self;
    fn add(&mut self, idx: usize,  page: Page) -> Self::Item;
    fn remove(&mut self, idx: usize);
}

pub trait Buffer {
    type Item;
    fn new(size: usize) -> Self;
    fn admit(&mut self, page: Page) -> Self::Item;
    fn evict(&mut self) -> Option<usize>;
    fn fetch(&mut self, page_id: u64) -> Self::Item;
    fn flush(&mut self);
}

pub struct ClockBuffer {
    hand: usize,
    vis: Vec<bool>,
    buf: PageStore
}

impl Buffer for ClockBuffer {

    type Item = Rc<RefCell<Page>>;

    fn new(size: usize) -> Self {
        ClockBuffer {
            hand: 0,
            vis: vec![true; size],
            buf: PageStore::new(size)
        }
    }

    fn admit(&mut self, page: Page) -> Self::Item {
        let target_idx = self.evict().expect("No page could be evicted");
        let res = self.buf.add(target_idx, page);
        self.vis[target_idx] = true;
        res
    }

    fn evict(&mut self) -> Option<usize> {
        for i in (self.hand..self.vis.len()).into_iter().chain((0..self.hand).into_iter()).cycle(){
            if !self.vis[i] {
                self.buf.remove(i); 
                self.vis[i] = true; 
                self.hand = i;
                return Some(i);
            }
            else {self.vis[i] = false;}
        };
        return None;
    }

    fn fetch(&mut self, p_id: u64) -> Rc<RefCell<Page>> {
        if let Some((idx, val)) = self.buf.iter().enumerate().find(|(_, p)| p.borrow().page_id == Some(p_id)) {
            self.vis[idx] = true;
            return Rc::clone(val);
        }
        let block = disk_manager::read_block(p_id);
        self.admit(
            Page { page_id: Some(p_id), block: Some(block) }
        )
    }

    fn flush(&mut self) {
        for i in 0..self.vis.len() {
            self.buf.remove(i);
        };
    }
}

pub struct PageStore(Vec<Rc<RefCell<Page>>>);

impl Store for PageStore {

    type Item = Rc<RefCell<Page>>;

    fn new(size: usize) -> Self{
        PageStore((0..size).into_iter().map(|_| Rc::new(RefCell::new(Page::default()))).collect())
    }

    fn add(&mut self, idx: usize, page: Page) -> Self::Item {
        self.0[idx].replace(page);
        Rc::clone(&self.0[idx])
    }

    fn remove(&mut self, idx: usize) {
        let mut p = self.0[idx].borrow_mut();
        if p.page_id.is_some() && p.is_dirty() {
            p.toggle_dirty();
            write_block(p.page_id.unwrap(), p.block.as_ref().unwrap());
        }
        p.page_id = None;
    }
}

impl PageStore {
    fn iter(&self) -> impl Iterator<Item = &Rc<RefCell<Page>>> {
        self.0.iter()
    }
}

impl Display for PageStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_list().entries(self.iter().map(|p| p.borrow().page_id).into_iter()).finish()
    }
}
