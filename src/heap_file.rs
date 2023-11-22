use crate::buffer_pool::PAGE_SIZE;
use crate::database;
use crate::heap_page::{HeapPage, HeapPageId};
use crate::tuple::{Tuple, TupleDesc};

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use uuid::Uuid;

pub struct HeapFile {
    file: File,
    td: TupleDesc,
    id: usize,
}

impl HeapFile {
    pub fn new(file: File, td: TupleDesc) -> Self {
        HeapFile {
            file,
            td,
            id: Uuid::new_v4().as_u128() as usize,
        }
    }

    pub fn get_file(&self) -> &File {
        &self.file
    }

    pub fn get_id(&self) -> usize {
        self.id
    }

    pub fn get_tuple_desc(&self) -> &TupleDesc {
        &self.td
    }

    pub fn read_page(&mut self, pid: HeapPageId) -> HeapPage {
        let mut data = vec![0; PAGE_SIZE];
        self.file
            .seek(SeekFrom::Start((pid.get_page_number() * PAGE_SIZE) as u64))
            .unwrap();
        self.file.read_exact(&mut data).unwrap();
        HeapPage::new(pid, data)
    }

    pub fn write_page(&mut self, page: HeapPage) {
        let pid = page.get_id();
        let data = page.get_page_data();
        self.file
            .seek(SeekFrom::Start((pid.get_page_number() * PAGE_SIZE) as u64))
            .unwrap();
        self.file.write_all(&data).unwrap();
    }

    pub fn num_pages(&self) -> usize {
        // returns the number of pages in this HeapFile
        self.file.metadata().unwrap().len() as usize / PAGE_SIZE
    }

    pub fn add_tuple(&mut self, tuple: Tuple) {
        // adds the specified tuple to the file
        let num_pages = self.num_pages();
        let table_id = self.get_id();
        let mut db = database::get_global_db();
        let bp = db.get_buffer_pool();
        for i in 0..num_pages {
            let pid = HeapPageId::new(table_id, i);
            let page = bp.get_page(pid).unwrap();
            if page.get_num_empty_slots() > 0 {
                page.add_tuple(tuple);
                return;
            }
        }
    }

    pub fn delete_tuple(&mut self, tuple: Tuple) {
        // deletes the specified tuple from the file
        let mut db = database::get_global_db();
        let bp = db.get_buffer_pool();
        let rid = tuple.get_record_id();
        let pid = rid.get_page_id();
        let page = bp.get_page(pid).unwrap();
        page.delete_tuple(tuple);
    }

    pub fn iter_mut(&mut self) -> HeapFileIterator {
        HeapFileIterator {
            heap_file: self,
            current_page_index: 0,
        }
    }
}

pub struct HeapFileIterator<'a> {
    heap_file: &'a mut HeapFile,
    current_page_index: usize,
}

impl<'a> Iterator for HeapFileIterator<'a> {
    type Item = HeapPage;

    fn next(&mut self) -> Option<Self::Item> {
        let num_pages = self.heap_file.num_pages();
        if self.current_page_index < num_pages {
            let pid = HeapPageId::new(self.heap_file.get_id(), self.current_page_index);
            let page = self.heap_file.read_page(pid);
            self.current_page_index += 1;
            Some(page)
        } else {
            None
        }
    }
}
