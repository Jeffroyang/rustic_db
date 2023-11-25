use crate::heap_file::HeapFile;
use crate::tuple::TupleDesc;
use crate::types::Type::{IntType, StringType};
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader};
use std::sync::{Arc, RwLock};

pub struct Catalog {
    // maps table name to table
    tables: RwLock<HashMap<String, Arc<HeapFile>>>,
    // maps table id to table
    table_ids: RwLock<HashMap<usize, Arc<HeapFile>>>,
    // maps table id to primary keys
    primary_keys: RwLock<HashMap<usize, String>>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: RwLock::new(HashMap::new()),
            table_ids: RwLock::new(HashMap::new()),
            primary_keys: RwLock::new(HashMap::new()),
        }
    }

    pub fn add_table_with_primary_key(&self, file: HeapFile, name: String, key: String) {
        let mut tables = self.tables.write().unwrap();
        let file_id = file.get_id();
        tables.insert(name.clone(), Arc::new(file));
        let mut table_ids = self.table_ids.write().unwrap();
        table_ids.insert(file_id, Arc::clone(tables.get(&name).unwrap()));
        let mut primary_keys = self.primary_keys.write().unwrap();
        primary_keys.insert(file_id, key);
    }

    // Adds a table to the catalog
    pub fn add_table(&self, file: HeapFile, name: String) {
        self.add_table_with_primary_key(file, name, "".to_string())
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
    pub fn load_schema(&self, schema_file_path: &str) {
        let schema_file = File::open(schema_file_path).unwrap();
        let reader = BufReader::new(schema_file);
        for line in reader.lines() {
            let line = line.unwrap();
            let split_parens: Vec<&str> = line.split('(').collect();
            let table_name = split_parens[0].to_string().replace(" ", "");
            let file = OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                .open(format!("data/{}.dat", table_name));

            let fields: Vec<&str> = split_parens[1].split(',').collect();
            let mut field_types = vec![];
            let mut field_names = vec![];
            for field in fields.iter() {
                let field: Vec<&str> = field.split(':').collect();
                let field_name = field[0].to_string().replace(" ", "");
                let field_type = field[1].to_string().replace(" ", "");
                let field_type = field_type.replace(")", "");
                let field_type = match field_type.as_str() {
                    "Int" => IntType,
                    "String" => StringType,
                    _ => panic!("invalid field type"),
                };
                field_names.push(field_name);
                field_types.push(field_type);
            }
            let heap_file = HeapFile::new(file.unwrap(), TupleDesc::new(field_types, field_names));
            self.add_table(heap_file, table_name);
        }
    }
}
