use crate::database;
use crate::heap_page::{HeapPage, HeapPageId};
use crate::tuple::Tuple;
use std::collections::HashMap;

pub const PAGE_SIZE: usize = 4096;
pub const DEFAULT_PAGES: usize = 50;

pub struct BufferPool {
    id_to_page: HashMap<HeapPageId, HeapPage>,
    num_pages: usize,
}

impl BufferPool {
    pub fn new() -> Self {
        BufferPool {
            id_to_page: HashMap::new(),
            num_pages: DEFAULT_PAGES,
        }
    }

    pub fn get_page(&mut self, pid: HeapPageId) -> Option<&mut HeapPage> {
        // TODO: add permissions and locking later
        self.id_to_page.get_mut(&pid)
    }

    pub fn insert_tuple(&mut self, table_id: usize, tuple: Tuple) {
        let mut db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.add_tuple(tuple);
    }

    pub fn delete_tuple(&mut self, table_id: usize, tuple: Tuple) {
        let mut db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.delete_tuple(tuple);
    }

    pub fn get_num_pages(&self) -> usize {
        self.num_pages
    }
}
