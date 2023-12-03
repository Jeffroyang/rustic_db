use crate::database;
use crate::heap_page::{HeapPage, HeapPageId, Permission};
use crate::lock_manager::LockManager;
use crate::transaction::TransactionId;
use crate::tuple::Tuple;
use std::collections::HashMap;

use std::sync::{Arc, RwLock};

pub const PAGE_SIZE: usize = 4096;
pub const DEFAULT_PAGES: usize = 50;

// Cache of pages kept in memory
pub struct BufferPool {
    id_to_page: RwLock<HashMap<HeapPageId, Arc<RwLock<HeapPage>>>>,
    lock_manager: RwLock<LockManager>,
    num_pages: usize,
}

impl BufferPool {
    pub fn new() -> Self {
        BufferPool {
            id_to_page: RwLock::new(HashMap::new()),
            num_pages: DEFAULT_PAGES,
            lock_manager: RwLock::new(LockManager::new()),
        }
    }

    // Retrieves the specified page from cache or disk
    pub fn get_page(
        &self,
        tid: TransactionId,
        pid: HeapPageId,
        perm: Permission,
    ) -> Option<Arc<RwLock<HeapPage>>> {
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

    // Releases all locks associated with the specified transaction
    pub fn release_locks(&self, tid: TransactionId) {
        // TODO: Implement Me!
        print!("Locks released for {:?}\n", tid);
        return;
    }

    // checks if the specified transaction has a lock on the specified page
    pub fn holds_lock(&self, tid: TransactionId, pid: HeapPageId) -> bool {
        // TODO: Implement Me!
        print!("Locks held for {:?}\n", tid);
        return false;
    }

    // Commits or aborts the specified transaction
    pub fn transaction_complete(&self, tid: TransactionId, commit: bool) {
        // TODO: Implement Me!
        print!("Transaction complete for {:?}\n", tid);
    }

    // Adds the tuple to the specified table
    pub fn insert_tuple(&self, tid: TransactionId, table_id: usize, tuple: Tuple) {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.add_tuple(tid, tuple);
    }

    // Deletes the tuple from the specified table
    pub fn delete_tuple(&mut self, tid: TransactionId, table_id: usize, tuple: Tuple) {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        // TODO: get table by record id
        let table = catalog.get_table_from_id(table_id).unwrap();
        table.delete_tuple(tid, tuple);
    }

    // Gets the number of pages in the buffer pool
    pub fn get_num_pages(&self) -> usize {
        self.num_pages
    }
}
