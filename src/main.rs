mod buffer_pool;
mod catalog;
mod database;
mod fields;
mod heap_file;
mod heap_page;
mod lock_manager;
mod transaction;
mod tuple;
mod types;

use std::thread;
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

    // 5. Insert 3 tuples into the employee table in 3 separate threads
    // threads panic if aborted by WAIT-DIE protocol
    print!("table id: {}\n", table_id);
    print!("table name: {:?}\n", td.get_field_name(0));
    let handles: Vec<_> = (0..3)
        .map(|_| {
            let db = database::get_global_db();
            let table = db.get_catalog().get_table_from_id(table_id).unwrap();
            let td = table.get_tuple_desc().clone();
            thread::spawn(move || loop {
                let res = std::panic::catch_unwind(|| {
                    let tid = transaction::TransactionId::new();
                    let bp = db.get_buffer_pool();
                    let name = format!("Alice_{}", tid.get_tid());
                    for i in 0..3 {
                        bp.insert_tuple(
                            tid,
                            table_id,
                            tuple::Tuple::new(
                                vec![
                                    fields::FieldVal::IntField(fields::IntField::new(i)),
                                    fields::FieldVal::StringField(fields::StringField::new(
                                        name.clone(),
                                        7,
                                    )),
                                ],
                                &td,
                            ),
                        );
                    }
                    bp.commit_transaction(tid);
                });
                if res.is_err() {
                    print!("thread {:?} aborted\n", thread::current().id());
                    thread::sleep(std::time::Duration::from_millis(500));
                } else {
                    print!("thread {:?} committed\n", thread::current().id());
                    break;
                }
            })
        })
        .collect();

    for handle in handles {
        handle.join().unwrap();
    }

    // 6. Print out the tuples in the employee table
    let mut tuple_count = 0;
    let mut page_count = 0;
    let tid = transaction::TransactionId::new();
    let table = catalog.get_table_from_id(table_id).unwrap();
    for page in table.iter(tid) {
        let page = page.read().unwrap();
        page_count += 1;
        for tuple in page.iter() {
            print!("tuple: {:?}\n", tuple);
            tuple_count += 1;
        }
    }
    let bp = db.get_buffer_pool();
    bp.commit_transaction(tid);

    print!("page count: {}\n", page_count);
    print!("tuple count: {}\n", tuple_count);
}
