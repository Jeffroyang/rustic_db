# rustic_db
A simple database management system inspired by [MIT Opencourseware](https://ocw.mit.edu/courses/6-830-database-systems-fall-2010/) implemented in Rust.

## Project Description
- The aim of our project is to create a basic relational database system. The system will be able to store and retrieve data. The user will also be able to perform basic table operations such as table creation and data operations such as insertion, filtering, projection, and joining.
- The database also allows for concurrent reads and writes through a transaction manager that guarantees atomicity
- We have also implemented the WAIT-DIE protocol for deadlock avoidance (Younger transactions are not allowed to wait on older transactions)
- The underlying structure of the data is stored in heapfiles, each representing one of our tables. Each heapfile consists of heappages for the table. The heappage consists of tuple data and a header bit mask that indicates the valid tuple slots on the page.

## Project Structure:
![SimpleDB](https://github.com/Jeffroyang/rustic_db/assets/82118995/2213c564-6b7c-4b62-99fb-0c298aebdf16)
- The buffer pool module is responsible for managing accessing page on disk and caching pages in memory for quicker access. It is also in charge of managing transactions in our database.
- The lock manager module is responsible for ensuring atomic transactions in our database. It also implements the WAIT-DIE protocol for deadlock avoidnace
- The heapfile module represents the underlying data for a data, and it communicates with the buffer pool in order to retrieve relevant pages. This provides a simple abstraction that allows us to easily query for pages.
- The database and catalog modules provide global variables that we can access. The database consists of both buffer pool and catalog fields. Having access to the catalog is useful for communicating what tables are available. Having access to the buffer pool allows us to commit transactions and allow heap files to easily access the pages needed.

