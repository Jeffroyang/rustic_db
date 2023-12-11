use crate::heap_file::{HeapFile, self};
use crate::tuple::TupleDesc;
use std::sync::Arc;
use crate::database; // Import the `database` module or crate
use crate::tuple; // Import the `tuple` module or crate
use crate::transaction; // Import the `transaction` module or crate
use crate::heap_page::HeapPageId;
use crate::heap_page; // Import the `heap_page` module or crate
use std::sync::RwLock;
use crate::tuple::Tuple;
use crate::fields::FieldVal;

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

    // produce an iterator that will iterate for count tuples, starting at the beginning of the table
    // unless given a fn, it will pretty much just return the tuple
    

    pub fn scan(&self, count: usize) -> TableIterator {
        let tid = transaction::TransactionId::new();
        TableIterator::new(self, tid, count)
    }

    
}


// ok so im just gonna make the iterator iterate on a view generated from the heapfile

pub struct TableIterator<'a> {
    table: &'a Table,
    current_page_index: usize,
    tid: transaction::TransactionId,
    data: Vec<tuple::Tuple>, // like a view ig
    filters: Vec<(String, Predicate)>,
}

impl<'a> TableIterator<'a> {


    // make a new table iterator and fill its vector with count tuples - 
    fn new(table: &'a Table, tid: transaction::TransactionId, count: usize) -> Self {
        let mut data = Vec::new();
        let mut count = count;
        for page in table.heap_file.iter(tid) {
            let page = page.read().unwrap();
            for tuple in page.iter() {
                if count == 0 {
                    break;
                }
                count -= 1;
                data.push(tuple.clone());
            }
        }
        TableIterator {
            table,
            current_page_index: 0,
            tid,
            data,
            filters: Vec::new(),
        }
    }

    pub fn project(&self, fields: Vec<String>) -> TableIterator {
        let mut data = Vec::new();
        let mut count = 0;

        
        // take the Tuple and make a new TupleDesc for it as well as a new Fields for it
        for tuple in self.data.iter() {
            let mut new_field_types = Vec::new();
            let mut new_field_vals = Vec::new();
    
            // go through each of the fields for this tuple
            for i in 0..tuple.get_tuple_desc().get_num_fields() {
                let field_name = tuple.get_tuple_desc().get_field_name(i).unwrap().clone();
    
                // Check if the field is in the list of fields to keep and has the desired type
                if fields.contains(&field_name) {
                    // we want to keep this field - so adding it to the new field types

                    let field_type = tuple.get_tuple_desc().get_field_type(i).unwrap().clone();
                    new_field_types.push(field_type);

                    let field = tuple.get_field(i).unwrap().clone();
                    new_field_vals.push(field);
                }
            }
    
            // Create a new tuple descriptor with only the selected fields
            let new_tuple_desc = TupleDesc::new(new_field_types, fields.clone());
    
            // Create a new tuple with the selected fields
            let new_tuple = Tuple::new(new_field_vals, &new_tuple_desc);
            
            data.push(new_tuple);
            count += 1;
        }
        // make a new iterator with the new data
        TableIterator {
            table: self.table,
            current_page_index: 0,
            tid: self.tid,
            data,
            filters: Vec::new(),
        }
    }

    
    pub fn table_filter(&mut self, field_name: &str, predicate: Predicate) {
        self.filters.push((field_name.to_string(), predicate));
    }
}

impl<'a> Iterator for TableIterator<'a> {
    type Item = tuple::Tuple;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_page_index < self.data.len() {
            let tuple = self.data[self.current_page_index].clone();
            self.current_page_index += 1;

            // also apply any filters here - dumb but i think it would work
            for filter in self.filters.iter() {
                print!("filtering: {:?}\n", filter.0);
                if !tuple.filter(&filter.0, &filter.1) {
                    return self.next();
                }
            }
            Some(tuple)
        } else {
            None
        }
    }


}

pub enum Predicate {
    Equals(String),
    GreaterThan(i32),
    LessThan(i32),
    // good enough for now
}

// trait to do filtering for filter()
pub trait Filterable {
    fn filter(&self, field_name: &str, predicate: &Predicate) -> bool;
}

// silly implementation of filter
impl Filterable for Tuple {
    fn filter(&self, field_name: &str, predicate: &Predicate) -> bool {

        for i in 0..self.get_tuple_desc().get_num_fields() {
            // iterating through all the fields in the tuple
            let field = self.get_field(i).unwrap();
            let t_field_name = self.get_tuple_desc().get_field_name(i).unwrap();
            if field_name == t_field_name {
                // found the field i want to filter
                match predicate {
                    Predicate::Equals(value) => {
                        if let FieldVal::StringField(string_field) = &field {
                            return string_field.get_value().as_str() == value;
                        } else {
                            return false;
                        }
                    }
                    Predicate::GreaterThan(value) => {
                        print!("field: {:?}\n", field.clone().into_int().unwrap().get_value());
                        print!("value: {:?}\n", value);
                        if field.clone().into_int().unwrap().get_value() > *value {
                            return true;
                        } else {
                            return false;
                        }
                    }
                    Predicate::LessThan(value) => {
                        if field.clone().into_int().unwrap().get_value() < *value {
                            return true;
                        } else {
                            return false;
                        }
                    }
                }
            }
        }
        false
    }
}

// impl<'a> Filterable for TableIterator<'a> {
//     fn filter(&self, field_name: &str, predicate: &Predicate) -> bool {
//         // Assuming your TableIterator contains a Tuple
//         self.next().map_or(false, |tuple| tuple.filter(field_name, predicate))
//     }
// }