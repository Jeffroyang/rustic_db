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
    let mut db = database::get_global_db();
    db.get_catalog().load_schema();
    let catalog = db.get_catalog();
    let table = catalog.get_table_from_name("employee").unwrap();
    let table_id = table.get_id();
    let td = table.get_tuple_desc().clone();
    print!("table id: {}\n", table.get_id());
    print!(
        "table name: {:?}\n",
        table.get_tuple_desc().get_field_name(0)
    );

    let bp = db.get_buffer_pool();
    bp.insert_tuple(
        table_id,
        tuple::Tuple::new(
            vec![
                fields::FieldVal::IntField(fields::IntField::new(1)),
                fields::FieldVal::StringField(fields::StringField::new("Alice".to_string(), 5)),
            ],
            &td,
        ),
    );
    print!("inserted tuple\n");

    bp.insert_tuple(
        table_id,
        tuple::Tuple::new(
            vec![
                fields::FieldVal::IntField(fields::IntField::new(2)),
                fields::FieldVal::StringField(fields::StringField::new("Bob".to_string(), 3)),
            ],
            &td,
        ),
    );

    print!("inserted tuple2\n");
    let pid = heap_page::HeapPageId::new(table_id, 0);
    let page = bp.get_page(pid).unwrap();
    for tuple in page.iter_mut() {
        print!("tuple: {:?}\n", tuple);
    }
}
