use crate::heap_file::HeapFile;
use crate::tuple::TupleDesc;
use crate::types::Type::{IntType, StringType};
use std::collections::HashMap;
use std::fs::OpenOptions;
use std::sync::{Arc, RwLock};

pub struct Catalog {
    // maps table name to table
    tables: RwLock<HashMap<String, Arc<HeapFile>>>,
    // maps table id to table
    table_ids: RwLock<HashMap<usize, Arc<HeapFile>>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: RwLock::new(HashMap::new()),
            table_ids: RwLock::new(HashMap::new()),
        }
    }

    // Adds a table to the catalog
    pub fn add_table(&self, file: HeapFile, name: String) {
        let mut tables = self.tables.write().unwrap();
        let file_id = file.get_id();
        tables.insert(name.clone(), Arc::new(file));
        let mut table_ids = self.table_ids.write().unwrap();
        table_ids.insert(file_id, Arc::clone(tables.get(&name).unwrap()));
    }

    // Retrieves the table with the specified name
    pub fn get_table_from_name(&self, name: &str) -> Option<Arc<HeapFile>> {
        let tables = self.tables.read().unwrap();
        match tables.get(name) {
            Some(t) => Some(Arc::clone(t)),
            None => None,
        }
    }

    // Retrieves the table with the specified id
    pub fn get_table_from_id(&self, id: usize) -> Option<Arc<HeapFile>> {
        let table_ids = self.table_ids.read().unwrap();
        match table_ids.get(&id) {
            Some(t) => Some(Arc::clone(t)),
            None => None,
        }
    }

    // Retrieves the tuple descriptor for the specified table
    pub fn get_tuple_desc(&self, table_id: usize) -> Option<TupleDesc> {
        let table = self.get_table_from_id(table_id);
        match table {
            Some(t) => Some(t.get_tuple_desc().clone()),
            None => None,
        }
    }

    // Loads the schema from a text file
    pub fn load_schema(&self) {
        // TODO: implement an acutal loader
        let name = "employee".to_string();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open("data/employee.dat");
        let heap_file = HeapFile::new(
            file.unwrap(),
            TupleDesc::new(
                vec![IntType, StringType],
                vec!["id".to_string(), "name".to_string()],
            ),
        );
        self.add_table(heap_file, name);
        print!("loaded employee schema");
    }
}
