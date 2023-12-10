use crate::heap_file::{HeapFile, self};
use std::sync::Arc;
use crate::database; // Import the `database` module or crate
use crate::tuple; // Import the `tuple` module or crate
use crate::transaction; // Import the `transaction` module or crate
use crate::heap_page::HeapPageId;

pub struct Table {
    name: String,
    heap_file: Arc<HeapFile>,
    table_id: usize,
    tuple_desc: tuple::TupleDesc,
}

impl Table {

    // let mut schema_file_path = std::env::current_dir().unwrap();
    // schema_file_path.push("schemas.txt");
    // db.get_catalog()
    //     .load_schema(schema_file_path.to_str().unwrap());
    pub fn new(name: String, schema: String) -> Self {
        let db = database::get_global_db();
        let catalog = db.get_catalog();
        
        // use the path given in schema to load the schema - should maybe do it differently
        let mut schema_file_path = std::env::current_dir().unwrap();
        schema_file_path.push(schema);

        let heap_file = catalog.get_table_from_name(&name).unwrap();
        let table_id = heap_file.get_id();

        Table {
            name,
            tuple_desc: heap_file.get_tuple_desc().clone(),
            heap_file,
            table_id,
        }
    }

    pub fn insert_tuple(&self, tuple: tuple::Tuple) {
        let db = database::get_global_db();
        let tid = transaction::TransactionId::new();
        let bp = db.get_buffer_pool();
        bp.insert_tuple(tid, self.table_id, tuple);
        bp.commit_transaction(tid);
    }

    pub fn get_tuple_desc(&self) -> &tuple::TupleDesc {
        &self.tuple_desc
    }

    pub fn print(&self) {
        let db = database::get_global_db();
        let mut tuple_count = 0;
        let tid = transaction::TransactionId::new();
        for page in self.heap_file.iter(tid) {
            let page = page.read().unwrap();
            tuple_count += 1;
            for tuple in page.iter() {
                print!("tuple: {:?}\n", tuple);
                tuple_count += 1;
            }
        }
        let bp = db.get_buffer_pool();
        bp.commit_transaction(tid);
    }
    
    // scan(5)
// a.scan(10)
// project(select)

// { id: int,  name: String }
// a.scan().project( { “name” })
// a.project( { “name” })

// scan should produce an iterator, project should take an iterator and apply a map to it where i am 


    pub fn scan(&self, count: usize) -> TableIterator {
        // produce a table iterator struct, that can then be called by project
        TableIterator {
            table: self,
            current_index: 0,
        }
    }

    
}



pub struct TableIterator<'a> {
    table: &'a Table,
    current_index: usize,
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = tuple::Tuple;

    
        fn next(&mut self) -> Option<tuple::Tuple> {
            let db = database::get_global_db();
            let tid = transaction::TransactionId::new();
            let bp = db.get_buffer_pool();
            
            for page in self.table.heap_file.iter(tid) {
                let page = page.read().unwrap();
                for tuple in page.iter() {
                    self.current_index += 1;
                    return Some(tuple.clone());
                }
                self.current_index += 1;
            }
            None
        }
        
    
}
