# rustic_db
A simple database management system inspired by [MIT Opencourseware](https://ocw.mit.edu/courses/6-830-database-systems-fall-2010/) implemented in Rust.


**Operations**
This Rust module provides a basic implementation of a relational table for a simple database system. The Table struct represents a table with properties like name, heap_file, table_id, and tuple_desc. Operations include inserting, scanning, and printing tuples. The TableIterator struct serves as an iterator for table views, supporting projection, filtering, and joining. Predicates like Equals, EqualsInt, GreaterThan, and LessThan facilitate filtering, while the Filterable trait adds filtering functionality to tuples. The code offers a means for a user to communicate with the actual database, demonstrating table creation, tuple insertion, scanning, and a join operation.
