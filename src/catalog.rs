use crate::heap_file::HeapFile;
use crate::tuple::TupleDesc;
use crate::types::Type::{IntType, StringType};
use std::collections::HashMap;
use std::fs::OpenOptions;

pub struct Catalog {
    // maps table name to table
    tables: HashMap<String, HeapFile>,
    // maps table id to table
    table_ids: HashMap<usize, HeapFile>,
}

impl Catalog {
    pub fn new() -> Self {
        Catalog {
            tables: HashMap::new(),
            table_ids: HashMap::new(),
        }
    }

    pub fn add_table(&mut self, file: HeapFile, name: String) {
        self.tables.insert(name.clone(), file);
    }

    pub fn get_table_from_name(&mut self, name: &str) -> Option<&mut HeapFile> {
        self.tables.get_mut(name)
    }

    pub fn get_table_from_id(&mut self, id: usize) -> Option<&mut HeapFile> {
        self.table_ids.get_mut(&id)
    }

    pub fn get_tuple_desc(&self, table_id: usize) -> Option<&TupleDesc> {
        self.table_ids.get(&table_id).map(|f| f.get_tuple_desc())
    }

    pub fn load_schema(&mut self) {
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_catalog() {
        let mut catalog = Catalog::new();
        catalog.load_schema();
        let table = catalog.get_table_from_name("employee").unwrap();
        assert_eq!(table.get_tuple_desc().get_num_fields(), 2);
        assert_eq!(table.get_tuple_desc().get_field_name(0).unwrap(), "id");
        assert_eq!(table.get_tuple_desc().get_field_name(1).unwrap(), "name");
        assert_eq!(table.get_tuple_desc().get_field_type(0).unwrap(), &IntType);
        assert_eq!(
            table.get_tuple_desc().get_field_type(1).unwrap(),
            &StringType
        );
    }
}
