mod buffer_pool;
mod catalog;
mod database;
mod fields;
mod heap_file;
mod heap_page;
mod transaction;
mod tuple;
mod types;

fn main() {
    let db = database::get_global_db();

    // 1. Load the schemas and tables from the schemas.txt file
    let mut schema_file_path = std::env::current_dir().unwrap();
    schema_file_path.push("schemas.txt");
    db.get_catalog()
        .load_schema(schema_file_path.to_str().unwrap());

    // 2. Retrieve the list of catalogs
    let catalog = db.get_catalog();

    // 3. Retrieve the table id for the employee table
    let table = catalog.get_table_from_name("employees").unwrap();
    let table_id = table.get_id();

    // 4. Retrieve the tuple descriptor for the employee table
    let td = table.get_tuple_desc().clone();

    print!("table id: {}\n", table_id);
    print!("table name: {:?}\n", td.get_field_name(0));

    // 5. Insert 1000 tuples into the employee table
    let bp = db.get_buffer_pool();
    for i in 0..1000 {
        bp.insert_tuple(
            table_id,
            tuple::Tuple::new(
                vec![
                    fields::FieldVal::IntField(fields::IntField::new(i)),
                    // insert random string
                    fields::FieldVal::StringField(fields::StringField::new("Alice".to_string(), 5)),
                ],
                &td,
            ),
        );
    }

    // 6. Print out the tuples in the employee table
    let table = catalog.get_table_from_id(table_id).unwrap();
    for page in table.iter() {
        for tuple in page.iter() {
            print!("tuple: {:?}\n", tuple);
        }
    }
    let pid = heap_page::HeapPageId::new(table_id, 0);
    bp.get_page(pid);
}
