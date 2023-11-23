use crate::database;
use crate::heap_page::{HeapPage, HeapPageId};
use crate::tuple::Tuple;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};

pub const PAGE_SIZE: usize = 4096;
pub const DEFAULT_PAGES: usize = 50;

// Cache of pages kept in memory
pub struct BufferPool {
    id_to_page: RwLock<HashMap<HeapPageId, Arc<RwLock<HeapPage>>>>,
    num_pages: usize,
}

impl BufferPool {
    pub fn new() -> Self {
        BufferPool {
            id_to_page: RwLock::new(HashMap::new()),
            num_pages: DEFAULT_PAGES,
        }
    }

    // Retrieves the specified page from cache or disk
    pub fn get_page(&self, pid: HeapPageId) -> Option<Arc<RwLock<HeapPage>>> {
        {
            let id_to_page = self.id_to_page.read().unwrap();
            if id_to_page.contains_key(&pid) {
                return Some(Arc::clone(id_to_page.get(&pid).unwrap()));
            }
        }
        // read the page from disk and saves it to the buffer pool
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(pid.get_table_id()).unwrap();
        let page = table.read_page(pid.clone());
        let mut id_to_page = self.id_to_page.write().unwrap();
        id_to_page.insert(pid.clone(), Arc::new(RwLock::new(page)));
        Some(Arc::clone(id_to_page.get(&pid).unwrap()))
    }

    // Adds the tuple to the specified table
    pub fn insert_tuple(&self, table_id: usize, tuple: Tuple) {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.add_tuple(tuple);
    }

    // Deletes the tuple from the specified table
    pub fn delete_tuple(&mut self, table_id: usize, tuple: Tuple) {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.delete_tuple(tuple);
    }

    // Gets the number of pages in the buffer pool
    pub fn get_num_pages(&self) -> usize {
        self.num_pages
    }
}
