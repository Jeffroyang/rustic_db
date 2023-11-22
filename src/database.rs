use crate::buffer_pool::BufferPool;
use crate::catalog::Catalog;
use lazy_static::lazy_static;
use std::sync::{Mutex, MutexGuard};

lazy_static! {
    static ref GLOBAL_DB: Mutex<Database> = Mutex::new(Database::new());
}

pub fn get_global_db() -> MutexGuard<'static, Database> {
    GLOBAL_DB.lock().unwrap()
}

pub struct Database {
    buffer_pool: BufferPool,
    catalog: Catalog,
}

impl Database {
    pub fn new() -> Self {
        Database {
            buffer_pool: BufferPool::new(),
            catalog: Catalog::new(),
        }
    }

    pub fn get_buffer_pool(&mut self) -> &mut BufferPool {
        &mut self.buffer_pool
    }

    pub fn get_catalog(&mut self) -> &mut Catalog {
        &mut self.catalog
    }
}
