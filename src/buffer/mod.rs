#![allow(dead_code)]

use std::{sync::{RwLock, Mutex}, marker::PhantomData, slice::Iter};

use crate::storage::disk_manager::{self, write_block};

pub mod page;
pub mod tuple;
use page::*;

pub trait BuffInner<T> {
    type Item;
    fn add(&self, idx: usize,  page: Self::Item) -> &T;
    fn remove(&self, idx: usize);
    fn iter(&self) -> Iter<'_, T>;
}

pub trait Buff<T> {
    type Item;
    fn admit(&self, page: Self::Item) -> &T;
    fn evict(&self) -> usize;
    fn fetch(&self, page_id: u64) -> &T;
    fn flush(&self);
}

pub struct Buffer<T, U: BuffInner<T>,  V: Keeper> {
    _marker: PhantomData<T>,
    inner: U,
    keeper: Mutex<V>,
    size: usize
}

impl Buffer<RwLock<Page>, BufferInner<RwLock<Page>>, Clock> {
    pub fn new(size: usize) -> Self {
        Self { _marker: PhantomData, inner: BufferInner::<RwLock<Page>>::new(size), keeper: Mutex::new(Clock::new(size)), size }
    }
}

pub struct BufferInner<T> {
    data: Vec<T>
}

impl BufferInner<RwLock<Page>> {
    fn new(size: usize) -> Self {
        Self { data: (0..size).into_iter().map(|_| RwLock::new(Page::new())).collect() }
    }
}

pub trait Keeper {
    fn add_hook(&mut self, idx: usize);
    fn fetch_hook(&mut self, idx: usize);
    fn evict(&mut self) -> usize; 
}

pub struct Clock {
    hand: usize,
    vis: Vec<bool>
}

impl Clock {
    fn new(size: usize) -> Self {
        Self { hand: 0, vis: vec![false; size] }
    }
}

impl Keeper for Clock {
    fn add_hook(&mut self, idx: usize) {
        self.vis[idx] = true;
    }

    fn fetch_hook(&mut self, idx: usize) {
        self.vis[idx] = true;
    }

    fn evict(&mut self) -> usize {
        for i in (self.hand..self.vis.len()).into_iter().chain((0..self.hand).into_iter()).cycle(){
            if !self.vis[i] {
                self.vis[i] = true; 
                self.hand = i;
                break;
            }
            else {self.vis[i] = false;}
        };
        self.hand
    }
}

impl<U: BuffInner<RwLock<Page>, Item = Page>, V: Keeper> Buff<RwLock<Page>> for Buffer<RwLock<Page>, U, V> {

    type Item = Page;

    fn admit(&self, page: Page) -> &RwLock<Page> {
        let target_idx = self.evict();
        let res = self.inner.add(target_idx, page);
        let mut keeper = self.keeper.lock().unwrap();
        keeper.add_hook(target_idx);
        res
    }

    fn evict(&self) -> usize {
        let mut keeper = self.keeper.lock().unwrap();
        let i = keeper.evict();
        self.inner.remove(i); 
        i
    }

    fn fetch(&self, p_id: u64) -> &RwLock<Page> {
        if let Some((idx, val)) = self.inner.iter().enumerate().find(|(_, p)| p.read().unwrap().page_id == Some(p_id)) {
            let mut keeper = self.keeper.lock().unwrap();
            keeper.fetch_hook(idx);
            return val;
        }
        let block = disk_manager::read_block(p_id);
        self.admit(
            Page { page_id: Some(p_id), block: Some(block) }
        )
    }

    fn flush(&self) {
        for i in 0..self.size {
            self.inner.remove(i);
        };
    }
}

impl BuffInner<RwLock<Page>> for BufferInner<RwLock<Page>> {

    type Item = Page;

    fn add(&self, idx: usize, page: Self::Item) -> &RwLock<Page> {
        let mut p = self.data[idx].write().unwrap();
        *p = page;
        &self.data[idx]
    }

    fn remove(&self, idx: usize) {
        let mut p = self.data[idx].write().unwrap();
        if p.page_id.is_some() && p.is_dirty() {
            p.toggle_dirty();
            write_block(p.page_id.unwrap(), p.block.as_ref().unwrap());
        }
        p.page_id = None;
    }

    fn iter(&self) -> Iter<'_, RwLock<Page>> {
        self.data.iter()
    }
}
