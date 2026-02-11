/*
use fbtree::{
    bp::{ContainerKey, MemPool, PageFrameKey},
    prelude::AccessMethodError,
};
use std::sync::Arc;

pub trait IndexTrait<T: MemPool> {
    type IndexIter: Iterator<Item = (u32, u64)>;

    fn new(bp: Arc<T>, c_key: ContainerKey) -> Self;

    fn load(&self, page_key: PageFrameKey) -> Self;

    fn insert(&mut self, key: u32, val: u64) -> Result<(), AccessMethodError>;

    fn get(&self, key: u32) -> Result<u64, AccessMethodError>;

    fn update(&mut self, key: u32, val: u64) -> Result<u64, AccessMethodError>;

    fn delete(&mut self, key: u32) -> Result<u64, AccessMethodError>;

    fn scan(&self) -> Result<Self::IndexIter, AccessMethodError>;

    fn range_scan(&self, min_key: u32, max_key: u32) -> Result<Self::IndexIter, AccessMethodError>;
}
*/
