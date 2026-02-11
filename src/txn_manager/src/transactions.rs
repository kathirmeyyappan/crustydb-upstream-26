use common::ids::TransactionId;
use common::CrustyError;

#[derive(Debug, PartialEq, Eq)]
pub enum TxnState {
    Active,
    PartiallyCommitted,
    Committed,
    Failed,
    Aborted,
    Terminated,
}

/// Transaction implementation.
pub struct Transaction {
    tid: TransactionId,
    state: TxnState,
}

impl Default for Transaction {
    fn default() -> Self {
        Self::new()
    }
}

impl Transaction {
    /// Creates a new transaction.
    pub fn new() -> Self {
        Self {
            tid: TransactionId::new(),
            state: TxnState::Active,
        }
    }

    pub fn new_from_tid(tid: TransactionId) -> Self {
        Self {
            tid,
            state: TxnState::Active,
        }
    }

    /// Returns the transaction id.
    pub fn tid(&self) -> Result<TransactionId, CrustyError> {
        if self.state != TxnState::Active {
            Err(CrustyError::TransactionNotActive)
        } else {
            Ok(self.tid)
        }
    }

    /// Commits the transaction.
    pub fn commit(&mut self) -> Result<(), CrustyError> {
        self.state = TxnState::PartiallyCommitted;
        self.complete()
    }

    /// Aborts the transaction.
    pub fn abort(&mut self) -> Result<(), CrustyError> {
        self.state = TxnState::Failed;
        self.complete()
    }

    /// Completes the transaction.
    ///
    /// # Arguments
    ///
    /// * `commit` - True if the transaction should commit.
    pub fn complete(&mut self) -> Result<(), CrustyError> {
        if self.state != TxnState::PartiallyCommitted || self.state != TxnState::Failed {
            error!(
                "Error: FIXME, need to notify on txn complete {:?}",
                self.state
            );
            //FIXME DBSERVER.transaction_complete(self.tid, commit)?;
            self.state = TxnState::Terminated;
        }
        Ok(())
    }
}

/*
#[cfg(test)]
mod test {
    use super::*;
    use crate::bufferpool::BufferPool;
    use crate::catalog::Catalog;
    use crate::storage::dbfile::DbFile;
    use crate::storage::heapfile::HeapFile;
    use crate::storage::heappage::HeapPage;
    use crate::storage::pageid::PageId;
    use crate::table::Table;
    use crate::testutil::test_util::*;
    use crate::DBSERVER;
    use common::{Field, Tuple};
    use std::sync::{Arc, Once};

    static WIDTH: usize = 3;
    static TABLE: &str = "TransactionsTable";
    static SETUP: Once = Once::new();

    // Returns the table_id of the test table
    fn setup() -> Result<Arc<HeapFile>, CrustyError> {
        SETUP.call_once(|| {
            let tuple = Tuple::new(vec![
                Field::Int(1),
                Field::Int(2),
                Field::Int(3),
            ]);
            let tid = TransactionId::new();
            let schema = get_int_table_schema(WIDTH);
            let table_ptr = create_empty_test_table(TABLE, schema.clone());
            let table_ref = table_ptr.read().unwrap();
            let hf = table_ref.file.as_ref().expect("No hf found").clone();
            // Create an empty heapfile and fill it with 3 pages
            for _ in 0..675 {
                hf.insert_tuple(tuple.clone(), tid).unwrap();
            }

            assert_eq!(3, hf.num_pages());

            DBSERVER.flush_all().unwrap();
            DBSERVER.buffer_pool_clear();
        });
        DBSERVER.buffer_pool_clear();
        let table_id = Table::get_table_id(TABLE);
        let table_ptr = connect_to_test_db().get_table_ptr(table_id)?;
        let table_ref = table_ptr.read().unwrap();
        Ok(table_ref.file.as_ref().unwrap().clone())
    }

    #[test]
    fn test_attempt_transaction_twice() -> Result<(), CrustyError> {
        let table_id = setup()?.get_file_id();
        let p0 = PageId::new(table_id, 0);
        let p1 = PageId::new(table_id, 1);
        let tid1 = TransactionId::new();
        let tid2 = TransactionId::new();

        DBSERVER.get_page(p0, tid1, Permissions::ReadOnly)?;
        DBSERVER.get_page(p1, tid2, Permissions::ReadOnly)?;
        DBSERVER.transaction_complete(tid1, true)?;

        DBSERVER.get_page(p0, tid2, Permissions::ReadWrite)?;
        DBSERVER.get_page(p1, tid2, Permissions::ReadWrite)?;
        Ok(())
    }

    fn test_transacation_complete(commit: bool) -> Result<(), CrustyError> {
        let table = setup()?;
        let p2 = PageId::new(table.get_file_id(), 2);
        let tid1 = TransactionId::new();
        let tid2 = TransactionId::new();

        let page = DBSERVER.get_page(p2, tid1, Permissions::ReadWrite)?;
        let t = Tuple::new(vec![
            // make tuple unique to current test run
            Field::Int(tid1.id() as i32 + tid2.id() as i32),
            Field::Int(tid2.id() as i32),
            Field::Int(tid1.id() as i32),
        ]);

        {
            let mut page_box = page.write().unwrap();
            let page_ref: &HeapPage = (**page_box).as_any().downcast_ref::<HeapPage>().unwrap();
            page_ref.insert_tuple(t.clone())?;
            page_box.mark_dirty(Some(tid1));
        }
        DBSERVER.transaction_complete(tid1, commit)?;

        DBSERVER.buffer_pool_clear();
        let page = DBSERVER.get_page(p2, tid2, Permissions::ReadWrite)?;
        let mut it;
        {
            let page_box = page.write().unwrap();
            let page_ref: &HeapPage = (**page_box).as_any().downcast_ref::<HeapPage>().unwrap();
            it = page_ref.iter();
        }
        let mut found = false;
        it.open();
        while let Some(tup) = it.next()? {
            if tup.get_field(0) == t.get_field(0)
                && tup.get_field(1) == t.get_field(1)
                && tup.get_field(2) == t.get_field(2)
            {
                found = true;
                break;
            }
        }
        assert_eq!(commit, found);
        Ok(())
    }

    #[test]
    fn test_commit_transaction() -> Result<(), CrustyError> {
        test_transacation_complete(true)
    }

    #[test]
    fn test_abort_transaction() -> Result<(), CrustyError> {
        test_transacation_complete(false)
    }

    mod locking {
        use super::*;
        use crate::bufferpool::BufferPool;
        use crate::storage::pageid::PageId;
        use crate::table::Table;
        use crate::DBSERVER;
        use common::{Field, Tuple};
        use std::sync::mpsc;
        use std::sync::Once;
        use std::thread;
        use std::time::Duration;

        static WIDTH: usize = 3;
        static TABLE: &str = "LockingTable";
        static SETUP: Once = Once::new();
        // time to wait before checking state of lock in ms
        static TIMEOUT: u64 = 100;

        // Returns the table_id of the test table
        fn setup() -> Result<u64, CrustyError> {
            SETUP.call_once(|| {
                let tuple = Tuple::new(vec![
                    Field::Int(1),
                    Field::Int(2),
                    Field::Int(3),
                ]);
                let tid = TransactionId::new();
                let schema = get_int_table_schema(WIDTH);
                let table_ptr = create_empty_test_table(TABLE, schema.clone());
                let table_ref = table_ptr.read().unwrap();
                let hf = match &table_ref.file {
                    Some(x) => x.clone(),
                    _ => panic!("No hf found"),
                };
                // Create an empty heapfile and fill it with 3 pages
                for _ in 0..675 {
                    hf.insert_tuple(tuple.clone(), tid).unwrap();
                }

                assert_eq!(3, hf.num_pages());

                DBSERVER.flush_all().unwrap();
                DBSERVER.buffer_pool_clear();
            });
            DBSERVER.buffer_pool_clear();
            let alias_id = Table::get_table_id(TABLE);
            Ok(alias_id)
        }

        fn meta_lock_tester(
            tid1: TransactionId,
            pid1: PageId,
            perm1: Permissions,
            tid2: TransactionId,
            pid2: PageId,
            perm2: Permissions,
            expected: bool,
        ) -> Result<(), CrustyError> {
            DBSERVER.get_page(pid1, tid1, perm1)?;
            grab_lock(tid2, pid2, perm2, expected)
        }

        fn grab_lock(
            tid: TransactionId,
            pid: PageId,
            perm: Permissions,
            expected: bool,
        ) -> Result<(), CrustyError> {
            let (send, recv) = mpsc::channel();
            thread::spawn(move || {
                connect_to_test_db();
                let res = DBSERVER.get_page(pid, tid, perm);
                send.send(res).unwrap();
                DBSERVER.transaction_complete(tid, false).unwrap();
            });

            let sent_page = recv.recv_timeout(Duration::from_millis(TIMEOUT));
            match sent_page {
                // Lock on page acquired
                Ok(Ok(_)) => assert!(expected),
                // Lock not acquired in time
                Err(_) | Ok(Err(CrustyError::TransactionAbortedError)) => assert!(!expected),
                // Error occured while attempting to acquire lock
                Ok(Err(e)) => {
                    return Err(e);
                }
            }
            Ok(())
        }

        #[test]
        fn acquire_read_locks_same_page() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadOnly,
                tid2,
                p0,
                Permissions::ReadOnly,
                true,
            )
        }

        #[test]
        fn acquire_read_write_locks_same_page() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadOnly,
                tid2,
                p0,
                Permissions::ReadWrite,
                false,
            )
        }

        #[test]
        fn acquire_write_read_locks_same_page() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadWrite,
                tid2,
                p0,
                Permissions::ReadOnly,
                false,
            )
        }

        #[test]
        fn acquire_read_write_locks_two_pages() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let p1 = PageId::new(table, 1);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            DBSERVER.buffer_pool_clear();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadOnly,
                tid2,
                p1,
                Permissions::ReadWrite,
                true,
            )
        }

        #[test]
        fn acquire_write_locks_two_pages() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let p1 = PageId::new(table, 1);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            DBSERVER.buffer_pool_clear();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadWrite,
                tid2,
                p1,
                Permissions::ReadWrite,
                true,
            )
        }

        #[test]
        fn acquire_read_locks_two_pages() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let p1 = PageId::new(table, 1);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            DBSERVER.buffer_pool_clear();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadOnly,
                tid2,
                p1,
                Permissions::ReadOnly,
                true,
            )
        }

        #[test]
        fn lock_upgrade() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let p1 = PageId::new(table, 1);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            DBSERVER.buffer_pool_clear();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadOnly,
                tid1,
                p0,
                Permissions::ReadWrite,
                true,
            )?;
            DBSERVER.get_page(p1, tid2, Permissions::ReadOnly)?;
            meta_lock_tester(
                tid2,
                p1,
                Permissions::ReadWrite,
                tid1,
                p1,
                Permissions::ReadOnly,
                false,
            )
        }

        #[test]
        fn acquire_write_and_read_locks() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let tid1 = TransactionId::new();
            DBSERVER.buffer_pool_clear();
            meta_lock_tester(
                tid1,
                p0,
                Permissions::ReadWrite,
                tid1,
                p0,
                Permissions::ReadOnly,
                true,
            )
        }

        #[test]
        fn acquire_then_release() -> Result<(), CrustyError> {
            let table = setup()?;
            let p0 = PageId::new(table, 0);
            let p1 = PageId::new(table, 1);
            let tid1 = TransactionId::new();
            let tid2 = TransactionId::new();
            DBSERVER.get_page(p0, tid1, Permissions::ReadWrite)?;
            DBSERVER.release_page(p0, tid1)?;
            DBSERVER.get_page(p0, tid2, Permissions::ReadWrite)?;

            DBSERVER.get_page(p1, tid2, Permissions::ReadWrite)?;
            DBSERVER.release_page(p1, tid2)?;
            DBSERVER.get_page(p1, tid1, Permissions::ReadWrite)?;
            Ok(())
        }
    }

    mod multi_thread {
        use super::*;
        use crate::bufferpool::BufferPool;
        use crate::catalog::Catalog;
        use crate::opiterator::{OpIterator, SeqScan};
        use crate::storage::heapfile::HeapFileIter;
        use crate::table::Table;
        use crate::DBSERVER;
        use common::{Field, Tuple};
        use std::sync::mpsc;
        use std::sync::{Arc, Barrier};
        use std::thread;
        use std::time::Duration;

        static TABLE: &str = "MultiThreadTxnTable";
        // 10 minutes
        static TIMEOUT: u64 = 10 * 60 * 1000;

        fn validate_transactions(threads: usize) -> Result<(), CrustyError> {
            // Create table with single tuple
            let alias = format!("{}_{}", TABLE, threads);
            let schema = get_int_table_schema(1);
            let table_ptr = create_empty_test_table(&alias, schema.clone());
            let table_id = Table::get_table_id(&alias);
            let tid1 = TransactionId::new();
            DBSERVER.insert_tuple(table_id, Tuple::new(vec![Field::Int(0)]), tid1)?;
            DBSERVER.transaction_complete(tid1, true)?;

            // Execute 'threads' number of transactions
            // Have each transaction increment tuple field by 1
            let (send, recv) = mpsc::channel();
            thread::spawn(move || {
                spawn_threads(threads, alias.clone(), send);
            });

            // Wait for all transactions to complete
            match recv.recv_timeout(Duration::from_millis(TIMEOUT)) {
                Ok(Ok(_)) => (),
                Ok(Err(e)) => {
                    return Err(e);
                }
                Err(_) => panic!("Timed out while waiting for transaction to complete"),
            }

            // Check that the table has the correct value
            let tid = TransactionId::new();
            let hf;
            {
                let table_ref = table_ptr.read().unwrap();
                hf = table_ref.file.as_ref().unwrap().clone();
            }
            let mut iter = HeapFileIter::new(hf, tid);
            iter.open();
            let tup = iter.next().unwrap();
            let actual = tup.get_field(0).unwrap().unwrap_int_field();
            assert_eq!(threads as i32, actual);
            iter.close();
            DBSERVER.transaction_complete(tid, true)
        }

        // Spawns all transaction threads and waits for them to complete
        // Sends Ok if all transactions committed successfully
        // Sends Err if any unexpected errors occured while executing transaction
        fn spawn_threads(
            threads: usize,
            alias: String,
            send: mpsc::Sender<Result<(), CrustyError>>,
        ) {
            let barrier = Arc::new(Barrier::new(threads));
            let mut handles = Vec::with_capacity(threads);
            for _ in 0..threads {
                let c = barrier.clone();
                let table = alias.clone();
                let mut txn = Transaction::new();
                let builder = thread::Builder::new().name(format!("{:?}", txn.tid()));
                let t = builder
                    .spawn(move || {
                        connect_to_test_db();
                        let mut error = Ok(());
                        c.wait();
                        while let Err(e) = attempt_transaction(&mut txn, &table) {
                            match e {
                                CrustyError::TransactionAbortedError => txn.abort().unwrap(),
                                _ => {
                                    error = Err(e);
                                    break;
                                }
                            }
                        }
                        // Return any unexpected error to parent thread
                        error
                    })
                    .unwrap();
                handles.push(t);
            }

            let mut res = Ok(());
            for handle in handles {
                // If any thread returns an error, save it in res
                res = res.and(handle.join().unwrap());
            }
            send.send(res).unwrap();
        }

        fn attempt_transaction(txn: &mut Transaction, table: &str) -> Result<(), CrustyError> {
            let table_id = Table::get_table_id(table);
            let table_ptr = DBSERVER.get_current_db()?.get_table_ptr(table_id)?;
            txn.start();

            let mut scan = SeqScan::new(table_ptr.clone(), table, txn.tid());
            scan.open()?;
            let orig_t = scan.next()?.unwrap();
            scan.close()?;
            let i = orig_t.get_field(0).unwrap().unwrap_int_field();

            // Create new tuple with incremented int field
            let mut new_t = orig_t.clone();
            new_t.set_field(0, Field::Int(i + 1));

            thread::sleep(Duration::from_millis(1));

            DBSERVER.delete_tuple(orig_t, txn.tid())?;
            DBSERVER.insert_tuple(table_id, new_t, txn.tid())?;

            txn.commit()
        }

        #[test]
        fn test_single_thread() -> Result<(), CrustyError> {
            validate_transactions(1)
        }

        #[test]
        fn test_two_threads() -> Result<(), CrustyError> {
            validate_transactions(2)
        }

        #[test]
        fn test_five_threads() -> Result<(), CrustyError> {
            validate_transactions(5)
        }

        #[ignore]
        #[test]
        fn test_ten_threads() -> Result<(), CrustyError> {
            validate_transactions(10)
        }
    }
}
*/
