# rustic_db
A simple database management system inspired by [MIT Opencourseware](https://ocw.mit.edu/courses/6-830-database-systems-fall-2010/) implemented in Rust.

## Project Description
- The aim of our project is to create a basic relational database system. The system will be able to store and retrieve data. The user will also be able to perform basic table operations such as table creation and data operations such as insertion, filtering, projection, and joining.
- The database also allows for concurrent reads and writes through a transaction manager that guarantees atomicity
- We have also implemented the WAIT-DIE protocol for deadlock avoidance (Younger transactions are not allowed to wait on older transactions)
- The underlying structure of the data is stored in heapfiles, each representing one of our tables. Each heapfile consists of heappages for the table. The heappage consists of tuple data and a header bit mask that indicates the valid tuple slots on the page.

## Project Structure:
### Database Engine
- The database engine is responsible for managing data storage, data retrieval, table/data operations, caches, and transactions. There are structs defined for Database, Catalogs, HeapFiles, Tuples, Tuple Descriptors, Fields, and Types.
