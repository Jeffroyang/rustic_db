use crate::heap_page::HeapPageId;
use crate::transaction::TransactionId;
use std::collections::HashMap;
use std::collections::HashSet;

#[derive(Debug, PartialEq, Eq, Clone, Hash, Copy)]
struct Lock {
    tid: TransactionId,
    exclusive: bool,
    pid: HeapPageId,
}

pub struct LockManager {
    page_locks: HashMap<HeapPageId, Lock>,
    transaction_locks: HashMap<TransactionId, Vec<Lock>>,
    wait_for_graph: HashMap<TransactionId, HashSet<TransactionId>>,
}

impl LockManager {
    pub fn new() -> Self {
        LockManager {
            page_locks: HashMap::new(),
            transaction_locks: HashMap::new(),
            wait_for_graph: HashMap::new(),
        }
    }
}
